package server

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrCouldNotWriteObject      = errors.New("could not write object to GCS")
	ErrCouldNotFinalizeObject   = errors.New("could not finalize object in GCS")
	ErrCouldNotFindBucket       = errors.New("could not find bucket in GCS")
	ErrCouldNotFindObject       = errors.New("could not find object in GCS")
	ErrCouldNotRetrieveMetadata = errors.New("could not retrieve metadata from GCS object")
	ErrCouldNotUpdateMetadata   = errors.New("could not update metadata from GCS object")
	ErrMissingExpiresTag        = errors.New("missing expires tag in GCS object")
	ErrInvalidExpiresTag        = errors.New("invalid expires tag in GCS object")
	ErrMissingLockIdTag         = errors.New("missing lock id tag in GCS object")
	ErrIncorrectLockIdTag       = errors.New("incorrect lock id")
	ErrCouldNotDeleteObject     = errors.New("could not delete object in GCS")
	ErrObjectIsNotMutex         = errors.New("object is not a mutex")
	ErrObjectIsExpired          = errors.New("object is expired")
	ErrIncorrectGeneration      = errors.New("incorrect generation")
)

var errToRPCCode = map[string]codes.Code{
	ErrCouldNotWriteObject.Error():      codes.Unavailable,
	ErrCouldNotFinalizeObject.Error():   codes.Unavailable,
	ErrCouldNotFindBucket.Error():       codes.NotFound,
	ErrCouldNotFindObject.Error():       codes.NotFound,
	ErrCouldNotRetrieveMetadata.Error(): codes.Unavailable,
	ErrCouldNotUpdateMetadata.Error():   codes.Canceled,
	ErrMissingExpiresTag.Error():        codes.InvalidArgument,
	ErrInvalidExpiresTag.Error():        codes.FailedPrecondition,
	ErrMissingLockIdTag.Error():         codes.InvalidArgument,
	ErrIncorrectLockIdTag.Error():       codes.InvalidArgument,
	ErrCouldNotDeleteObject.Error():     codes.Unavailable,
	ErrObjectIsNotMutex.Error():         codes.InvalidArgument,
	ErrObjectIsExpired.Error():          codes.FailedPrecondition,
	ErrIncorrectGeneration.Error():      codes.InvalidArgument,
}

type GRPCLockError struct {
	Err   error
	Chain error
}

func (e *GRPCLockError) Error() string {
	if e.Chain != nil {
		return fmt.Sprintf("%s (%s)", e.Err, e.Chain)
	}
	return fmt.Sprintf("%s", e.Err)
}

func (e *GRPCLockError) GRPCError() error {
	if code, ok := errToRPCCode[e.Err.Error()]; ok {
		return status.Error(code, e.Error())
	}
	return status.Error(codes.Unknown, e.Error())
}
