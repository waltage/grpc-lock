package server

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/waltage/grpc_lock/golang/protos"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	MAGIC_BYTES      = []byte("GRPC_LOCK")
	MAGIC_CRC32C     = crc32.Checksum(MAGIC_BYTES, crc32.MakeTable(crc32.Castagnoli))
	LOCK_ID_TAG_NAME = "grpc-lock-id"
	EXPIRES_TAG_NAME = "grpc-lock-expires"
)

type GCSCreateMutexArgs struct {
	// Original gRPC context
	Context context.Context
	// GCS bucket name
	Bucket string
	// GCS object name
	Object string
	// Expiration time for the lock.  Any attempts to create, access, or
	// certify the lock after this time will cause the object to be
	// Garbage Collected.
	Expires *timestamppb.Timestamp
	// A Google Cloud Storage Client
	Client *storage.Client
}

// GCSCreateMutex creates a new object in GCS with MAGIC and the associated attribute tags.
func GCSCreateMutex(args GCSCreateMutexArgs) (*protos.GCSMutex, *GRPCLockError) {
	// Create a new object in GCS iff the object does not already exist (Generation = 0).
	// The object is created with a magic byte sequence that can be used to
	// identify the object as a lock managed by this service.
	// Additionally, the object is created with a set of metadata tags that
	// can be used to identify loosely confirm ownership and expiration.
	//   - LOCK_ID_TAG_NAME: A UUIDv4 string that uniquely identifies the lock.
	//   - EXPIRES_TAG_NAME: A timestamp that indicates when the lock will be GC'd.

	obj := args.Client.Bucket(args.Bucket).Object(args.Object)
	objWriter := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(args.Context)

	expiresTag := fmt.Sprintf("%d.%d", args.Expires.GetSeconds(), args.Expires.GetNanos())
	lockIdTag := uuid.NewString()

	objWriter.ObjectAttrs.Metadata = make(map[string]string)
	objWriter.ObjectAttrs.Metadata[LOCK_ID_TAG_NAME] = lockIdTag
	objWriter.ObjectAttrs.Metadata[EXPIRES_TAG_NAME] = expiresTag

	if _, err := objWriter.Write(MAGIC_BYTES); err != nil {
		log.Default().Printf("could not write magic bytes to object: %v", err)
		return nil, &GRPCLockError{
			Err:   ErrCouldNotWriteObject,
			Chain: err,
		}
	}

	err := objWriter.Close()

	if err != nil {
		log.Default().Printf("could not finalize object: %v", err)
		return nil, &GRPCLockError{
			Err:   ErrCouldNotFinalizeObject,
			Chain: err,
		}
	}

	generationId := objWriter.Attrs().Generation

	return &protos.GCSMutex{
		Bucket:     args.Bucket,
		Object:     args.Object,
		Generation: generationId,
		LockId:     lockIdTag,
		Expires:    args.Expires,
	}, nil
}

type GCSDeleteMutexIfExpiredArgs struct {
	Context context.Context
	Bucket  string
	Object  string
	Client  *storage.Client
}

// GCSDeleteMutexIfExpired deletes a GCS object if the object has expired.
func GCSDeleteMutexIfExpired(args GCSDeleteMutexIfExpiredArgs) *GRPCLockError {
	obj := args.Client.Bucket(args.Bucket).Object(args.Object)
	objAttrs, err := obj.Attrs(args.Context)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil
		}
		if errors.Is(err, storage.ErrBucketNotExist) {
			return &GRPCLockError{
				Err:   ErrCouldNotFindBucket,
				Chain: err,
			}
		}
		return &GRPCLockError{
			Err:   ErrCouldNotRetrieveMetadata,
			Chain: err,
		}
	}

	if !ObjectIsMutex(objAttrs) {
		return nil
	}

	if !ObjectIsExpired(objAttrs) {
		return nil
	}
	if err := obj.Delete(args.Context); err != nil {
		return &GRPCLockError{
			Err:   ErrCouldNotDeleteObject,
			Chain: err,
		}
	}
	return nil
}

func GCSCertifyMutex(ctx context.Context, mutex *protos.GCSMutex, client *storage.Client) *GRPCLockError {
	obj := client.Bucket(mutex.Bucket).Object(mutex.Object)
	objAttrs, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return &GRPCLockError{
				Err:   ErrCouldNotFindObject,
				Chain: err,
			}
		}
		return &GRPCLockError{
			Err:   ErrCouldNotRetrieveMetadata,
			Chain: err,
		}
	}

	if !ObjectIsMutex(objAttrs) {
		return &GRPCLockError{
			Err: ErrObjectIsNotMutex,
		}
	}

	if ObjectIsExpired(objAttrs) {
		// delete
		err := obj.Delete(context.Background())
		return &GRPCLockError{
			Err:   ErrObjectIsExpired,
			Chain: err,
		}
	}

	if objAttrs.Metadata[LOCK_ID_TAG_NAME] != mutex.LockId {
		return &GRPCLockError{
			Err: ErrIncorrectLockIdTag,
		}
	}

	if objAttrs.Generation != mutex.Generation {
		return &GRPCLockError{
			Err: ErrIncorrectGeneration,
		}
	}

	updatedExpiration, err := ParseExpiresTag(objAttrs.Metadata[EXPIRES_TAG_NAME])
	if err != nil {
		return &GRPCLockError{
			Err: ErrInvalidExpiresTag,
		}
	}

	mutex.Expires = updatedExpiration

	return nil
}

type GCSExtendMutexArgs struct {
	Context  context.Context
	Mutex    *protos.GCSMutex
	Duration *durationpb.Duration
	Client   *storage.Client
}

func GCSExtendMutex(args GCSExtendMutexArgs) *GRPCLockError {
	rpcErr := GCSCertifyMutex(args.Context, args.Mutex, args.Client)
	if rpcErr != nil {
		return rpcErr
	}
	newTimestamp := time.Now().Add(args.Duration.AsDuration())
	newExpires := timestamppb.New(newTimestamp)
	obj := args.Client.Bucket(args.Mutex.Bucket).Object(args.Mutex.Object)
	atts, err := obj.Attrs(args.Context)
	if err != nil {
		return &GRPCLockError{
			Err:   ErrCouldNotRetrieveMetadata,
			Chain: err,
		}

	}
	newMetadata := atts.Metadata
	newMetadata[EXPIRES_TAG_NAME] = fmt.Sprintf("%d.%d", newExpires.GetSeconds(), newExpires.GetNanos())
	_, err = obj.Update(args.Context, storage.ObjectAttrsToUpdate{
		Metadata: newMetadata,
	})
	if err != nil {
		return &GRPCLockError{
			Err:   ErrCouldNotUpdateMetadata,
			Chain: err,
		}
	}
	args.Mutex.Expires = newExpires
	return nil
}

func GCSDeleteMutex(ctx context.Context, mutex *protos.GCSMutex, client *storage.Client) *GRPCLockError {
	rpcErr := GCSCertifyMutex(ctx, mutex, client)
	if rpcErr != nil {
		return rpcErr
	}
	obj := client.Bucket(mutex.Bucket).Object(mutex.Object)
	err := obj.Delete(ctx)
	if err != nil {
		return &GRPCLockError{
			Err:   ErrCouldNotDeleteObject,
			Chain: err,
		}
	}
	return nil
}

func ParseExpiresTag(tag string) (*timestamppb.Timestamp, error) {
	parts := strings.Split(tag, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("incorrect number of parts in expires tag: %s", tag)
	}
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing int seconds: %s (%s)", parts[0], err)
	}
	nanos, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("error parsing int nanos: %s (%s)", parts[1], err)
	}
	return &timestamppb.Timestamp{
		Seconds: seconds,
		Nanos:   int32(nanos),
	}, nil
}

func ObjectIsMutex(obj *storage.ObjectAttrs) bool {

	if _, ok := obj.Metadata[LOCK_ID_TAG_NAME]; !ok {
		return false
	}

	if _, ok := obj.Metadata[EXPIRES_TAG_NAME]; !ok {
		return false
	}

	if obj.CRC32C != MAGIC_CRC32C {
		return false
	}

	return true
}

func ObjectIsExpired(obj *storage.ObjectAttrs) bool {
	expires, err := ParseExpiresTag(obj.Metadata[EXPIRES_TAG_NAME])
	if err != nil {
		return false
	}
	return expires.AsTime().Before(time.Now())
}
