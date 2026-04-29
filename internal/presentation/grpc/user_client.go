package grpc

import (
	"context"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	userv1 "github.com/ofm-microseervices/ofm-common/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type userClient struct {
	conn *grpc.ClientConn
	cl   userv1.UserQueryServiceClient
	log  logging.Logger
}

// NewUserClient constructs the outbound user-service query client.
func NewUserClient(address string, log logging.Logger) (*userClient, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &userClient{
		conn: conn,
		cl:   userv1.NewUserQueryServiceClient(conn),
		log:  log.With(logging.String("module", "grpc-user-client"), logging.String("address", address)),
	}, nil
}

// ExistsByUsername checks whether user-service already owns the supplied
// username.
func (c *userClient) ExistsByUsername(ctx context.Context, username string) (bool, error) {
	response, err := c.cl.ExistsByUsername(ctx, &userv1.ExistsByUsernameRequest{Username: username})
	if err != nil {
		return false, err
	}

	return response.GetExists(), nil
}

// Close closes the underlying user-service gRPC connection.
func (c *userClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.log.Info("closing user query grpc client")
	return c.conn.Close()
}
