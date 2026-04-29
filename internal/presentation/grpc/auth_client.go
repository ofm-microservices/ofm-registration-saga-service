package grpc

import (
	"context"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	authv1 "github.com/ofm-microseervices/ofm-common/proto/auth/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type authClient struct {
	conn *grpc.ClientConn
	cl   authv1.AuthQueryServiceClient
	log  logging.Logger
}

// NewAuthClient constructs the outbound auth-service query client.
func NewAuthClient(address string, log logging.Logger) (*authClient, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &authClient{
		conn: conn,
		cl:   authv1.NewAuthQueryServiceClient(conn),
		log:  log.With(logging.String("module", "grpc-auth-client"), logging.String("address", address)),
	}, nil
}

// ExistsByEmail checks whether auth-service already owns the supplied email.
func (c *authClient) ExistsByEmail(ctx context.Context, email string) (bool, error) {
	response, err := c.cl.ExistsByEmail(ctx, &authv1.ExistsByEmailRequest{Email: email})
	if err != nil {
		return false, err
	}

	return response.GetExists(), nil
}

// Close closes the underlying auth-service gRPC connection.
func (c *authClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.log.Info("closing auth query grpc client")
	return c.conn.Close()
}
