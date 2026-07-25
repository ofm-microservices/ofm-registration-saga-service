package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	authv1 "github.com/ofm-microservices/ofm-common/proto/auth/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithUnaryInterceptor(metrics.UnaryClientInterceptor()),
	)
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

// VerifyRegistrationEmail verifies the code owned by auth-service.
func (c *authClient) VerifyRegistrationEmail(ctx context.Context, userID, code string) error {
	_, err := c.cl.VerifyRegistrationEmail(ctx, &authv1.VerifyRegistrationEmailRequest{UserId: userID, Code: code})
	return err
}

// DeactivateRegistrationAuth marks auth data inactive as saga compensation.
func (c *authClient) DeactivateRegistrationAuth(ctx context.Context, userID string) error {
	_, err := c.cl.DeactivateRegistrationAuth(ctx, &authv1.DeactivateRegistrationAuthRequest{UserId: userID})
	return err
}

// Close closes the underlying auth-service gRPC connection.
func (c *authClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.log.Info("closing auth query grpc client")
	return c.conn.Close()
}
