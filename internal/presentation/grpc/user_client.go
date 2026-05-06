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

// ActivateUser marks the saga-created user profile active.
func (c *userClient) ActivateUser(ctx context.Context, userID string) error {
	_, err := c.cl.ActivateUser(ctx, &userv1.ActivateUserRequest{UserId: userID})
	return err
}

// DeactivateUser marks the saga-created user profile inactive as compensation.
func (c *userClient) DeactivateUser(ctx context.Context, userID string) error {
	_, err := c.cl.DeactivateUser(ctx, &userv1.DeactivateUserRequest{UserId: userID})
	return err
}

// Close closes the underlying user-service gRPC connection.
func (c *userClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.log.Info("closing user query grpc client")
	return c.conn.Close()
}
