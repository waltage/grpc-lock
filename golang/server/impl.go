package server

import (
	"context"
	"net"
	"time"

	"cloud.google.com/go/storage"
	"github.com/waltage/grpc_lock/golang/protos"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GrpcLockServer struct {
	Listener   net.Listener
	GrpcServer *grpc.Server
	GCSClient  *storage.Client
	protos.UnimplementedGrpcLockServiceServer
}

func (s *GrpcLockServer) Serve() {
	protos.RegisterGrpcLockServiceServer(s.GrpcServer, s)
	s.GrpcServer.Serve(s.Listener)
}

func (s *GrpcLockServer) NewMutex(ctx context.Context, request *protos.NewMutexRequest) (*protos.GCSMutex, error) {
	// Garbage Collect
	errDel := GCSDeleteMutexIfExpired(
		GCSDeleteMutexIfExpiredArgs{
			Context: ctx,
			Bucket:  request.Bucket,
			Object:  request.Object,
			Client:  s.GCSClient,
		},
	)

	if errDel != nil {
		return nil, errDel.GRPCError()
	}

	// Create a new Mutex
	expires := time.Now().Add(time.Duration(request.Duration.Seconds*1e9 + int64(request.Duration.Nanos)))
	response, errCreate := GCSCreateMutex(
		GCSCreateMutexArgs{
			Context: ctx,
			Bucket:  request.Bucket,
			Object:  request.Object,
			Expires: timestamppb.New(expires),
			Client:  s.GCSClient,
		},
	)
	if errCreate != nil {
		return nil, errCreate.GRPCError()
	}
	return response, nil
}

func (s *GrpcLockServer) ExtendMutex(ctx context.Context, request *protos.ExtendMutexRequest) (*protos.GCSMutex, error) {
	err := GCSExtendMutex(
		GCSExtendMutexArgs{
			Context:  ctx,
			Mutex:    request.Mutex,
			Duration: request.Duration,
			Client:   s.GCSClient,
		},
	)
	if err != nil {
		return nil, err.GRPCError()
	}
	return request.Mutex, nil
}

func (s *GrpcLockServer) CertifyMutex(ctx context.Context, request *protos.GCSMutex) (*protos.GCSMutex, error) {
	err := GCSCertifyMutex(ctx, request, s.GCSClient)
	if err != nil {
		return nil, err.GRPCError()
	}
	return request, nil
}

func (s *GrpcLockServer) ReleaseMutex(ctx context.Context, request *protos.GCSMutex) (*emptypb.Empty, error) {
	err := GCSDeleteMutex(ctx, request, s.GCSClient)
	if err != nil {
		return nil, err.GRPCError()
	}
	return &emptypb.Empty{}, nil
}
