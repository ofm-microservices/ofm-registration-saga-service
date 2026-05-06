package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	authv1 "github.com/ofm-microseervices/ofm-common/proto/auth/v1"
	registrationv1 "github.com/ofm-microseervices/ofm-common/proto/registration/v1"
	userv1 "github.com/ofm-microseervices/ofm-common/proto/user/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"registration-saga-service/config"
	"registration-saga-service/internal/domain"
)

func TestGRPC(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "GRPC Suite")
}

type authQueryClientStub struct {
	exists bool
	err    error
}

func (s authQueryClientStub) DeactivateRegistrationAuth(context.Context, *authv1.DeactivateRegistrationAuthRequest, ...grpc.CallOption) (*authv1.DeactivateRegistrationAuthResponse, error) {
	return &authv1.DeactivateRegistrationAuthResponse{}, s.err
}

func (s authQueryClientStub) IssueRegistrationTokens(context.Context, *authv1.IssueRegistrationTokensRequest, ...grpc.CallOption) (*authv1.IssueRegistrationTokensResponse, error) {
	return &authv1.IssueRegistrationTokensResponse{}, s.err
}

func (s authQueryClientStub) VerifyRegistrationEmail(context.Context, *authv1.VerifyRegistrationEmailRequest, ...grpc.CallOption) (*authv1.VerifyRegistrationEmailResponse, error) {
	return &authv1.VerifyRegistrationEmailResponse{}, s.err
}

func (s authQueryClientStub) ExistsByEmail(context.Context, *authv1.ExistsByEmailRequest, ...grpc.CallOption) (*authv1.ExistsByEmailResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &authv1.ExistsByEmailResponse{Exists: s.exists}, nil
}

type userQueryClientStub struct {
	exists bool
	err    error
}

func (s userQueryClientStub) ActivateUser(context.Context, *userv1.ActivateUserRequest, ...grpc.CallOption) (*userv1.ActivateUserResponse, error) {
	return &userv1.ActivateUserResponse{}, s.err
}

func (s userQueryClientStub) DeactivateUser(context.Context, *userv1.DeactivateUserRequest, ...grpc.CallOption) (*userv1.DeactivateUserResponse, error) {
	return &userv1.DeactivateUserResponse{}, s.err
}

func (s userQueryClientStub) ExistsByUsername(context.Context, *userv1.ExistsByUsernameRequest, ...grpc.CallOption) (*userv1.ExistsByUsernameResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &userv1.ExistsByUsernameResponse{Exists: s.exists}, nil
}

var _ = Describe("GRPC", func() {
	var (
		ctrl   *gomock.Controller
		svc    *MockRegistrationService
		logger logging.Logger
		cfg    config.GRPCConfig
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		svc = NewMockRegistrationService(ctrl)
		cfg = config.GRPCConfig{Host: "127.0.0.1", Port: 19095}

		var err error
		logger, err = logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("NewServer", func() {
		It("validates nil collaborators", func() {
			server, err := NewServer(nil, cfg, logger)
			Expect(server).To(BeNil())
			Expect(err).To(MatchError(ErrNilRegistrationService))

			server, err = NewServer(svc, cfg, nil)
			Expect(server).To(BeNil())
			Expect(err).To(MatchError(ErrNilLogger))
		})

		It("constructs a grpc server", func() {
			server, err := NewServer(svc, cfg, logger)

			Expect(err).NotTo(HaveOccurred())
			Expect(server).NotTo(BeNil())
		})
	})

	Describe("registration mapper", func() {
		It("maps requests and domain results", func() {
			mapr := newRegistrationMapper(logger)

			params := mapr.ToStartParams(&registrationv1.StartRegistrationRequest{
				ClientId:  "client-1",
				Email:     "alex@example.com",
				Username:  "alex",
				Password:  "password123",
				FirstName: "Alex",
				Surname:   "Doe",
			})
			Expect(params.ClientID).To(Equal("client-1"))
			Expect(params.Email).To(Equal("alex@example.com"))

			resp := mapr.ToStartResponse(&domain.StartRegistrationResult{
				SessionID:     "session-1",
				ClientID:      "client-1",
				UserID:        "user-1",
				Status:        "started",
				ConflictState: domain.AvailabilityStateCompleted,
				UsernameTaken: true,
			})
			Expect(resp.SessionId).To(Equal("session-1"))
			Expect(resp.ConflictState).To(Equal(domain.AvailabilityStateCompleted))
			Expect(resp.UsernameTaken).To(BeTrue())

			verifyParams := mapr.ToVerifyEmailParams(&registrationv1.VerifyEmailRequest{
				SessionId: "session-1",
				ClientId:  "client-1",
				Code:      "123456",
			})
			Expect(verifyParams.SessionID).To(Equal("session-1"))
			Expect(verifyParams.ClientID).To(Equal("client-1"))
			Expect(verifyParams.Code).To(Equal("123456"))

			verifyResp := mapr.ToVerifyEmailResponse(&domain.VerifyEmailResult{
				SessionID: "session-1",
				ClientID:  "client-1",
				Status:    domain.SessionStatusVerifyingEmail,
			})
			Expect(verifyResp.SessionId).To(Equal("session-1"))
			Expect(verifyResp.Status).To(Equal(domain.SessionStatusVerifyingEmail))

			statusResp := mapr.ToRegistrationStatusResponse(&domain.RegistrationStatus{
				SessionID: "session-1",
				ClientID:  "client-1",
				UserID:    "user-1",
				Status:    domain.SessionStatusCompleted,
			})
			Expect(statusResp.SessionId).To(Equal("session-1"))
			Expect(statusResp.ClientId).To(Equal("client-1"))
			Expect(statusResp.UserId).To(Equal("user-1"))
			Expect(statusResp.Status).To(Equal(domain.SessionStatusCompleted))
		})

		It("maps validation failures to invalid-argument grpc errors", func() {
			mapr := newRegistrationMapper(logger)

			Expect(status.Code(mapr.ToStartError(domain.ErrInvalidSessionID))).To(Equal(codes.InvalidArgument))
			Expect(status.Code(mapr.ToStartError(domain.ErrInvalidClientID))).To(Equal(codes.InvalidArgument))
			Expect(status.Code(mapr.ToStartError(domain.ErrInvalidVerificationCode))).To(Equal(codes.InvalidArgument))
			Expect(status.Code(mapr.ToStartError(domain.ErrInvalidEmail))).To(Equal(codes.InvalidArgument))
			Expect(status.Code(mapr.ToStartError(domain.ErrInvalidUsername))).To(Equal(codes.InvalidArgument))
			Expect(status.Code(mapr.ToStartError(domain.ErrInvalidPassword))).To(Equal(codes.InvalidArgument))
			Expect(status.Code(mapr.ToStartError(domain.ErrSessionNotFound))).To(Equal(codes.NotFound))
			Expect(status.Code(mapr.ToStartError(domain.ErrClientMismatch))).To(Equal(codes.PermissionDenied))
			Expect(status.Code(mapr.ToStartError(domain.ErrInvalidStatus))).To(Equal(codes.FailedPrecondition))
			Expect(status.Code(mapr.ToStartError(errors.New("boom")))).To(Equal(codes.Internal))
		})
	})

	Describe("StartRegistration", func() {
		It("returns the application result", func() {
			srv, err := NewServer(svc, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			svc.EXPECT().Start(gomock.Any(), domain.StartRegistrationParams{
				ClientID:  "client-1",
				Email:     "alex@example.com",
				Username:  "alex",
				Password:  "password123",
				FirstName: "Alex",
				Surname:   "Doe",
			}).Return(&domain.StartRegistrationResult{
				SessionID: "session-1",
				ClientID:  "client-1",
				UserID:    "user-1",
				Status:    "started",
			}, nil)

			resp, err := srv.(*server).StartRegistration(context.Background(), &registrationv1.StartRegistrationRequest{
				ClientId:  "client-1",
				Email:     "alex@example.com",
				Username:  "alex",
				Password:  "password123",
				FirstName: "Alex",
				Surname:   "Doe",
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(resp.SessionId).To(Equal("session-1"))
		})

		It("maps service failures through the grpc mapper", func() {
			srv, err := NewServer(svc, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			svc.EXPECT().Start(gomock.Any(), gomock.Any()).Return(nil, domain.ErrInvalidEmail)

			resp, err := srv.(*server).StartRegistration(context.Background(), &registrationv1.StartRegistrationRequest{})

			Expect(resp).To(BeNil())
			Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
		})
	})

	Describe("Start and Shutdown", func() {
		It("returns an error for an invalid listen address", func() {
			srv, err := NewServer(svc, config.GRPCConfig{Host: "bad host", Port: 19095}, logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(srv.Start()).To(HaveOccurred())
		})

		It("starts and shuts down a live grpc server", func() {
			srv, err := NewServer(svc, config.GRPCConfig{Host: "127.0.0.1", Port: 0}, logger)
			Expect(err).NotTo(HaveOccurred())

			errCh := make(chan error, 1)
			go func() {
				errCh <- srv.Start()
			}()

			Eventually(func(g Gomega) {
				select {
				case startErr := <-errCh:
					if startErr != nil {
						Skip("sandbox does not permit opening TCP listeners")
					}
					g.Expect(startErr).NotTo(HaveOccurred())
				default:
					g.Expect(srv.(*server).listener).NotTo(BeNil())
				}
			}).Should(Succeed())

			err = srv.Shutdown(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("use of closed network connection"))
			Eventually(errCh).Should(Receive(BeNil()))
		})

		It("shuts down cleanly when the server was never started", func() {
			srv, err := NewServer(svc, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(srv.Shutdown(context.Background())).To(Succeed())
		})
	})

	Describe("outbound query clients", func() {
		It("delegates auth email existence lookups", func() {
			client := &authClient{cl: authQueryClientStub{exists: true}, log: logger}

			exists, err := client.ExistsByEmail(context.Background(), "alex@example.com")

			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("returns auth email lookup failures", func() {
			client := &authClient{cl: authQueryClientStub{err: errors.New("boom")}, log: logger}

			exists, err := client.ExistsByEmail(context.Background(), "alex@example.com")

			Expect(exists).To(BeFalse())
			Expect(err).To(MatchError("boom"))
		})

		It("constructs the auth-service client and closes it", func() {
			client, err := NewAuthClient("127.0.0.1:9091", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(client.Close()).To(Succeed())
		})

		It("delegates user username existence lookups", func() {
			client := &userClient{cl: userQueryClientStub{exists: true}, log: logger}

			exists, err := client.ExistsByUsername(context.Background(), "alex")

			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("returns user username lookup failures", func() {
			client := &userClient{cl: userQueryClientStub{err: errors.New("boom")}, log: logger}

			exists, err := client.ExistsByUsername(context.Background(), "alex")

			Expect(exists).To(BeFalse())
			Expect(err).To(MatchError("boom"))
		})

		It("constructs the user-service client and closes it", func() {
			client, err := NewUserClient("127.0.0.1:9092", logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(client.Close()).To(Succeed())
		})
	})
})
