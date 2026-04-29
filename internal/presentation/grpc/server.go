package grpc

import (
	"context"
	"fmt"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	registrationv1 "github.com/ofm-microseervices/ofm-common/proto/registration/v1"
	"net"
	"registration-saga-service/config"

	"google.golang.org/grpc"
)

type server struct {
	registrationv1.UnimplementedRegistrationServiceServer
	svc      RegistrationService
	cfg      config.GRPCConfig
	mapr     RegistrationMapper
	log      logging.Logger
	srv      *grpc.Server
	listener net.Listener
}

// NewServer constructs the registration-saga gRPC server.
func NewServer(svc RegistrationService, cfg config.GRPCConfig, log Logger) (Server, error) {
	if svc == nil {
		return nil, ErrNilRegistrationService
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	grpcSrv := grpc.NewServer()
	s := &server{
		svc:  svc,
		cfg:  cfg,
		mapr: newRegistrationMapper(log.With(logging.String("module", "grpc-registration-mapper"))),
		log:  log.With(logging.String("module", "grpc-server")),
		srv:  grpcSrv,
	}
	registrationv1.RegisterRegistrationServiceServer(grpcSrv, s)
	return s, nil
}

// Start begins serving the registration gRPC API.
func (s *server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.listener = lis
	s.log.Info("starting grpc server", logging.String("addr", addr))
	return s.srv.Serve(lis)
}

// Shutdown gracefully stops the gRPC server.
func (s *server) Shutdown(context.Context) error {
	if s.srv != nil {
		s.log.Info("shutting down grpc server")
		s.srv.GracefulStop()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// StartRegistration exposes the registration start use case over gRPC.
func (s *server) StartRegistration(ctx context.Context, req *registrationv1.StartRegistrationRequest) (*registrationv1.StartRegistrationResponse, error) {
	result, err := s.svc.Start(ctx, s.mapr.ToStartParams(req))
	if err != nil {
		return nil, s.mapr.ToStartError(err)
	}

	return s.mapr.ToStartResponse(result), nil
}
