package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"registration-saga-service/config"
	domain "registration-saga-service/internal/domain"
)

func TestApplication(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Application Suite")
}

var _ = Describe("RegistrationService", func() {
	var (
		ctrl            *gomock.Controller
		sessions        *MockSessionRepository
		steps           *MockStepRepository
		broker          *MockEventBroker
		emailChecker    *MockEmailAvailabilityChecker
		usernameChecker *MockUsernameAvailabilityChecker
		mapr            *MockRegistrationMessageMapper
		logger          logging.Logger
		natsCfg         config.NATSConfig
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		sessions = NewMockSessionRepository(ctrl)
		steps = NewMockStepRepository(ctrl)
		broker = NewMockEventBroker(ctrl)
		emailChecker = NewMockEmailAvailabilityChecker(ctrl)
		usernameChecker = NewMockUsernameAvailabilityChecker(ctrl)
		mapr = NewMockRegistrationMessageMapper(ctrl)
		natsCfg = config.NATSConfig{
			UserCreateSubject:           "saga.user.create",
			AuthCreatePendingSubject:    "saga.auth.create_pending",
			RegistrationCodeSentSubject: "registration.code.sent",
		}

		var err error
		logger, err = logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	newService := func() *registrationService {
		svc, err := New(sessions, steps, broker, emailChecker, usernameChecker, natsCfg, logger)
		Expect(err).NotTo(HaveOccurred())
		return svc.(*registrationService)
	}

	Describe("New", func() {
		It("validates nil collaborators", func() {
			svc, err := New(nil, steps, broker, emailChecker, usernameChecker, natsCfg, logger)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilSessionRepository))

			svc, err = New(sessions, nil, broker, emailChecker, usernameChecker, natsCfg, logger)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilStepRepository))

			svc, err = New(sessions, steps, nil, emailChecker, usernameChecker, natsCfg, logger)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilEventBroker))

			svc, err = New(sessions, steps, broker, nil, usernameChecker, natsCfg, logger)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilEmailChecker))

			svc, err = New(sessions, steps, broker, emailChecker, nil, natsCfg, logger)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilUsernameChecker))

			svc, err = New(sessions, steps, broker, emailChecker, usernameChecker, natsCfg, nil)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilLogger))
		})
	})

	Describe("normalizeStartInput", func() {
		It("rejects invalid inputs", func() {
			_, err := normalizeStartInput(domain.StartRegistrationParams{Email: "bad"})
			Expect(err).To(MatchError(domain.ErrInvalidEmail))

			_, err = normalizeStartInput(domain.StartRegistrationParams{
				Email:    "alex@example.com",
				Password: "password123",
			})
			Expect(err).To(MatchError(domain.ErrInvalidUsername))

			_, err = normalizeStartInput(domain.StartRegistrationParams{
				Email:    "alex@example.com",
				Username: "alex",
				Password: "short",
			})
			Expect(err).To(MatchError(domain.ErrInvalidPassword))
		})

		It("trims fields and generates a client id when missing", func() {
			input, err := normalizeStartInput(domain.StartRegistrationParams{
				Email:     " alex@example.com ",
				Username:  " alex ",
				Password:  " password123 ",
				FirstName: " Alex ",
				Surname:   " Doe ",
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(input.email).To(Equal("alex@example.com"))
			Expect(input.username).To(Equal("alex"))
			Expect(input.password).To(Equal("password123"))
			Expect(input.firstName).To(Equal("Alex"))
			Expect(input.surname).To(Equal("Doe"))
			Expect(input.clientID).NotTo(BeEmpty())
		})
	})

	Describe("lookupStartConflict", func() {
		It("returns an incomplete conflict before checking external services", func() {
			svc := newService()

			emailSession := &domain.Session{SessionID: "session-1", Status: domain.SessionStatusStarted}
			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(emailSession, nil)
			sessions.EXPECT().GetByUsername(gomock.Any(), "alex").Return(nil, domain.ErrSessionNotFound)

			result, err := svc.lookupStartConflict(context.Background(), "alex@example.com", "alex")

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.ConflictState).To(Equal(domain.AvailabilityStateIncomplete))
			Expect(result.EmailTaken).To(BeTrue())
		})

		It("falls back to completed-conflict checks when no incomplete session exists", func() {
			svc := newService()

			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(nil, domain.ErrSessionNotFound)
			sessions.EXPECT().GetByUsername(gomock.Any(), "alex").Return(nil, domain.ErrSessionNotFound)
			emailChecker.EXPECT().ExistsByEmail(gomock.Any(), "alex@example.com").Return(false, nil)
			usernameChecker.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(true, nil)

			result, err := svc.lookupStartConflict(context.Background(), "alex@example.com", "alex")

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.ConflictState).To(Equal(domain.AvailabilityStateCompleted))
			Expect(result.UsernameTaken).To(BeTrue())
		})

		It("returns incomplete lookup failures", func() {
			svc := newService()
			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(nil, errors.New("db failed"))

			result, err := svc.lookupStartConflict(context.Background(), "alex@example.com", "alex")

			Expect(result).To(BeNil())
			Expect(err).To(MatchError("db failed"))
		})

		It("returns completed conflict checker failures", func() {
			svc := newService()
			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(nil, domain.ErrSessionNotFound)
			sessions.EXPECT().GetByUsername(gomock.Any(), "alex").Return(nil, domain.ErrSessionNotFound)
			emailChecker.EXPECT().ExistsByEmail(gomock.Any(), "alex@example.com").Return(false, errors.New("auth failed"))

			result, err := svc.lookupStartConflict(context.Background(), "alex@example.com", "alex")

			Expect(result).To(BeNil())
			Expect(err).To(MatchError("auth failed"))
		})
	})

	Describe("createStartSession", func() {
		It("creates a started session with generated ids", func() {
			svc := newService()
			input := startInput{
				email:    "alex@example.com",
				username: "alex",
				clientID: "client-1",
			}

			sessions.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(domain.Session{})).
				DoAndReturn(func(_ context.Context, session domain.Session) (*domain.Session, error) {
					Expect(session.SessionID).NotTo(BeEmpty())
					Expect(session.UserID).NotTo(BeEmpty())
					Expect(session.ClientID).To(Equal("client-1"))
					Expect(session.Status).To(Equal(domain.SessionStatusStarted))
					return &session, nil
				})

			session, err := svc.createStartSession(context.Background(), input)

			Expect(err).NotTo(HaveOccurred())
			Expect(session.Email).To(Equal("alex@example.com"))
			Expect(session.Username).To(Equal("alex"))
		})
	})

	Describe("createStartSteps", func() {
		It("creates the three initial steps", func() {
			svc := newService()

			gomock.InOrder(
				steps.EXPECT().Create(gomock.Any(), domain.Step{
					SessionID: "session-1",
					StepKey:   domain.StepKeyUserCreateProfile,
					Status:    domain.StepStatusPending,
				}).Return(&domain.Step{}, nil),
				steps.EXPECT().Create(gomock.Any(), domain.Step{
					SessionID: "session-1",
					StepKey:   domain.StepKeyAuthCreatePending,
					Status:    domain.StepStatusPending,
				}).Return(&domain.Step{}, nil),
				steps.EXPECT().Create(gomock.Any(), domain.Step{
					SessionID: "session-1",
					StepKey:   domain.StepKeyMailSendVerificationCode,
					Status:    domain.StepStatusPending,
				}).Return(&domain.Step{}, nil),
			)

			Expect(svc.createStartSteps(context.Background(), "session-1")).To(Succeed())
		})
	})

	Describe("dispatchStartCommands", func() {
		It("marks the session in progress and publishes both commands", func() {
			svc := newService()
			svc.mapr = mapr

			session := domain.Session{SessionID: "session-1", ClientID: "client-1", UserID: "user-1", Username: "alex"}
			input := startInput{email: "alex@example.com", firstName: "Alex", surname: "Doe"}

			gomock.InOrder(
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusInProgress).Return(nil),
				mapr.EXPECT().ToUserCreateCommandPayload(session, "Alex", "Doe").Return([]byte(`{"kind":"user"}`), nil),
				broker.EXPECT().Publish(gomock.Any(), natsCfg.UserCreateSubject, []byte(`{"kind":"user"}`)).Return(nil),
				mapr.EXPECT().ToAuthCreatePendingCommandPayload(session, "alex@example.com", "hash").Return([]byte(`{"kind":"auth"}`), nil),
				broker.EXPECT().Publish(gomock.Any(), natsCfg.AuthCreatePendingSubject, []byte(`{"kind":"auth"}`)).Return(nil),
			)

			svc.dispatchStartCommands(session, input, "hash")
		})

		It("marks the session as failed when user command publication fails", func() {
			svc := newService()
			svc.mapr = mapr

			session := domain.Session{SessionID: "session-1"}

			gomock.InOrder(
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusInProgress).Return(nil),
				mapr.EXPECT().ToUserCreateCommandPayload(session, "", "").Return(nil, errors.New("marshal failed")),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusFailed).Return(nil),
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusFailed).Return(nil),
			)

			svc.dispatchStartCommands(session, startInput{}, "hash")
		})

		It("marks the auth step as failed when auth publication fails", func() {
			svc := newService()
			svc.mapr = mapr

			session := domain.Session{SessionID: "session-1"}

			gomock.InOrder(
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusInProgress).Return(nil),
				mapr.EXPECT().ToUserCreateCommandPayload(session, "", "").Return([]byte(`{"kind":"user"}`), nil),
				broker.EXPECT().Publish(gomock.Any(), natsCfg.UserCreateSubject, []byte(`{"kind":"user"}`)).Return(nil),
				mapr.EXPECT().ToAuthCreatePendingCommandPayload(session, "", "hash").Return(nil, errors.New("marshal auth failed")),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusFailed).Return(nil),
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusFailed).Return(nil),
			)

			svc.dispatchStartCommands(session, startInput{}, "hash")
		})

		It("stops dispatching when marking the session in progress fails", func() {
			svc := newService()
			svc.mapr = mapr

			sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusInProgress).Return(errors.New("status failed"))

			svc.dispatchStartCommands(domain.Session{SessionID: "session-1"}, startInput{}, "hash")
		})

		It("stops when marking the user step in progress fails", func() {
			svc := newService()

			gomock.InOrder(
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusInProgress).Return(errors.New("user step failed")),
			)

			svc.dispatchStartCommands(domain.Session{SessionID: "session-1"}, startInput{}, "hash")
		})

		It("stops when marking the auth step in progress fails", func() {
			svc := newService()

			gomock.InOrder(
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusInProgress).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusInProgress).Return(errors.New("auth step failed")),
			)

			svc.dispatchStartCommands(domain.Session{SessionID: "session-1"}, startInput{}, "hash")
		})
	})

	Describe("HandleUserCreateResult", func() {
		It("rejects an empty session id", func() {
			svc := newService()

			err := svc.HandleUserCreateResult(context.Background(), UserCreateResult{})

			Expect(err).To(MatchError(domain.ErrInvalidSessionID))
		})

		It("marks the user step complete and syncs session state on success", func() {
			svc := newService()

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusCompleted).Return(nil),
				steps.EXPECT().ListBySessionID(gomock.Any(), "session-1").Return([]domain.Step{
					{StepKey: domain.StepKeyUserCreateProfile, Status: domain.StepStatusCompleted},
					{StepKey: domain.StepKeyAuthCreatePending, Status: domain.StepStatusCompleted},
					{StepKey: domain.StepKeyMailSendVerificationCode, Status: domain.StepStatusCompleted},
				}, nil),
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusCodeSent).Return(nil),
			)

			Expect(svc.HandleUserCreateResult(context.Background(), UserCreateResult{
				SessionID: "session-1",
				Status:    "success",
			})).To(Succeed())
		})

		It("marks the session failed on non-success status", func() {
			svc := newService()

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyUserCreateProfile, domain.StepStatusFailed).Return(nil),
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusFailed).Return(nil),
			)

			Expect(svc.HandleUserCreateResult(context.Background(), UserCreateResult{
				SessionID: "session-1",
				Status:    "failed",
			})).To(Succeed())
		})
	})

	Describe("HandleAuthCreatePendingResult", func() {
		It("rejects an empty session id", func() {
			svc := newService()

			err := svc.HandleAuthCreatePendingResult(context.Background(), AuthCreatePendingResult{})

			Expect(err).To(MatchError(domain.ErrInvalidSessionID))
		})

		It("activates the mail step after a successful auth result", func() {
			svc := newService()

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusCompleted).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyMailSendVerificationCode, domain.StepStatusInProgress).Return(nil),
				steps.EXPECT().ListBySessionID(gomock.Any(), "session-1").Return([]domain.Step{
					{StepKey: domain.StepKeyUserCreateProfile, Status: domain.StepStatusCompleted},
					{StepKey: domain.StepKeyAuthCreatePending, Status: domain.StepStatusCompleted},
					{StepKey: domain.StepKeyMailSendVerificationCode, Status: domain.StepStatusInProgress},
				}, nil),
			)

			Expect(svc.HandleAuthCreatePendingResult(context.Background(), AuthCreatePendingResult{
				SessionID: "session-1",
				Status:    "success",
			})).To(Succeed())
		})

		It("marks the session failed on non-success status", func() {
			svc := newService()

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusFailed).Return(nil),
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusFailed).Return(nil),
			)

			Expect(svc.HandleAuthCreatePendingResult(context.Background(), AuthCreatePendingResult{
				SessionID: "session-1",
				Status:    "failed",
			})).To(Succeed())
		})

		It("returns mail-step update failures after a successful auth result", func() {
			svc := newService()

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyAuthCreatePending, domain.StepStatusCompleted).Return(nil),
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyMailSendVerificationCode, domain.StepStatusInProgress).Return(errors.New("mail step failed")),
			)

			err := svc.HandleAuthCreatePendingResult(context.Background(), AuthCreatePendingResult{
				SessionID: "session-1",
				Status:    "success",
			})

			Expect(err).To(MatchError("mail step failed"))
		})
	})

	Describe("HandleMailSendResult", func() {
		It("rejects an empty session id", func() {
			svc := newService()

			err := svc.HandleMailSendResult(context.Background(), MailSendResult{})

			Expect(err).To(MatchError(domain.ErrInvalidSessionID))
		})

		It("publishes the code-sent event and completes the session when all steps succeeded", func() {
			svc := newService()
			svc.mapr = mapr
			result := MailSendResult{SessionID: "session-1", ClientID: "client-1", UserID: "user-1", Status: "success"}

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyMailSendVerificationCode, domain.StepStatusCompleted).Return(nil),
				mapr.EXPECT().ToCodeSentEventPayload(result).Return([]byte(`{"status":"code_sent"}`), nil),
				broker.EXPECT().Publish(gomock.Any(), natsCfg.RegistrationCodeSentSubject, []byte(`{"status":"code_sent"}`)).Return(nil),
				steps.EXPECT().ListBySessionID(gomock.Any(), "session-1").Return([]domain.Step{
					{StepKey: domain.StepKeyUserCreateProfile, Status: domain.StepStatusCompleted},
					{StepKey: domain.StepKeyAuthCreatePending, Status: domain.StepStatusCompleted},
					{StepKey: domain.StepKeyMailSendVerificationCode, Status: domain.StepStatusCompleted},
				}, nil),
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusCodeSent).Return(nil),
			)

			Expect(svc.HandleMailSendResult(context.Background(), result)).To(Succeed())
		})

		It("marks the session failed when mail delivery fails", func() {
			svc := newService()

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyMailSendVerificationCode, domain.StepStatusFailed).Return(nil),
				sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusFailed).Return(nil),
			)

			Expect(svc.HandleMailSendResult(context.Background(), MailSendResult{
				SessionID: "session-1",
				Status:    "failed",
			})).To(Succeed())
		})

		It("returns code-sent payload mapping failures", func() {
			svc := newService()
			svc.mapr = mapr
			result := MailSendResult{SessionID: "session-1", ClientID: "client-1", UserID: "user-1", Status: "success"}

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyMailSendVerificationCode, domain.StepStatusCompleted).Return(nil),
				mapr.EXPECT().ToCodeSentEventPayload(result).Return(nil, errors.New("marshal failed")),
			)

			err := svc.HandleMailSendResult(context.Background(), result)
			Expect(err).To(MatchError("marshal failed"))
		})

		It("returns code-sent publish failures", func() {
			svc := newService()
			svc.mapr = mapr
			result := MailSendResult{SessionID: "session-1", ClientID: "client-1", UserID: "user-1", Status: "success"}

			gomock.InOrder(
				steps.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.StepKeyMailSendVerificationCode, domain.StepStatusCompleted).Return(nil),
				mapr.EXPECT().ToCodeSentEventPayload(result).Return([]byte(`{"status":"code_sent"}`), nil),
				broker.EXPECT().Publish(gomock.Any(), natsCfg.RegistrationCodeSentSubject, []byte(`{"status":"code_sent"}`)).Return(errors.New("publish failed")),
			)

			err := svc.HandleMailSendResult(context.Background(), result)
			Expect(err).To(MatchError("publish failed"))
		})
	})

	Describe("syncSessionStatus", func() {
		It("marks the session failed when any step failed", func() {
			svc := newService()

			steps.EXPECT().ListBySessionID(gomock.Any(), "session-1").Return([]domain.Step{
				{StepKey: domain.StepKeyUserCreateProfile, Status: domain.StepStatusFailed},
			}, nil)
			sessions.EXPECT().UpdateStatus(gomock.Any(), "session-1", domain.SessionStatusFailed).Return(nil)

			Expect(svc.syncSessionStatus(context.Background(), "session-1")).To(Succeed())
		})

		It("does nothing while some steps are still pending", func() {
			svc := newService()

			steps.EXPECT().ListBySessionID(gomock.Any(), "session-1").Return([]domain.Step{
				{StepKey: domain.StepKeyUserCreateProfile, Status: domain.StepStatusCompleted},
				{StepKey: domain.StepKeyAuthCreatePending, Status: domain.StepStatusPending},
			}, nil)

			Expect(svc.syncSessionStatus(context.Background(), "session-1")).To(Succeed())
		})

		It("returns step listing failures", func() {
			svc := newService()

			steps.EXPECT().ListBySessionID(gomock.Any(), "session-1").Return(nil, errors.New("list failed"))

			Expect(svc.syncSessionStatus(context.Background(), "session-1")).To(MatchError("list failed"))
		})
	})

	Describe("message mapper", func() {
		It("builds outbound payloads and conflict results", func() {
			m := newRegistrationMessageMapper()
			session := domain.Session{
				SessionID: "session-1",
				ClientID:  "client-1",
				UserID:    "user-1",
				Username:  "alex",
			}

			userPayload, err := m.ToUserCreateCommandPayload(session, "Alex", "Doe")
			Expect(err).NotTo(HaveOccurred())
			Expect(string(userPayload)).To(ContainSubstring(`"username":"alex"`))

			authPayload, err := m.ToAuthCreatePendingCommandPayload(session, "alex@example.com", "hash")
			Expect(err).NotTo(HaveOccurred())
			Expect(string(authPayload)).To(ContainSubstring(`"email":"alex@example.com"`))

			codeSentPayload, err := m.ToCodeSentEventPayload(MailSendResult{
				SessionID: "session-1",
				ClientID:  "client-1",
				UserID:    "user-1",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(string(codeSentPayload)).To(ContainSubstring(`"status":"code_sent"`))

			Expect(m.ToIncompleteConflictResult(
				&domain.Session{Status: domain.SessionStatusStarted},
				nil,
			).ConflictState).To(Equal(domain.AvailabilityStateIncomplete))

			Expect(m.ToCompletedConflictResult(false, false)).To(BeNil())
			Expect(m.ToCompletedConflictResult(true, false).ConflictState).To(Equal(domain.AvailabilityStateCompleted))
		})
	})

	Describe("error wrappers", func() {
		It("wraps application helper failures", func() {
			cause := errors.New("boom")
			Expect(WrapHashPasswordError(cause)).To(MatchError(ContainSubstring("hash password")))
			Expect(WrapPublishUserCreateCommandError(cause)).To(MatchError(ContainSubstring("publish user create command")))
			Expect(WrapPublishAuthCreateCommandError(cause)).To(MatchError(ContainSubstring("publish auth create pending registration command")))
			Expect(WrapPublishCodeSentEventError(cause)).To(MatchError(ContainSubstring("publish registration code sent event")))
		})
	})

	Describe("hashStartPassword", func() {
		It("returns a bcrypt hash", func() {
			hash, err := hashStartPassword("password123")

			Expect(err).NotTo(HaveOccurred())
			Expect(hash).NotTo(BeEmpty())
			Expect(hash).NotTo(Equal("password123"))
		})
	})

	Describe("Start", func() {
		It("returns normalized input validation failures", func() {
			svc := newService()

			result, err := svc.Start(context.Background(), domain.StartRegistrationParams{
				Email:    "bad-email",
				Username: "alex",
				Password: "password123",
			})

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(domain.ErrInvalidEmail))
		})

		It("returns a conflict without creating saga state", func() {
			svc := newService()

			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(&domain.Session{
				SessionID: "session-1",
				Status:    domain.SessionStatusStarted,
			}, nil)
			sessions.EXPECT().GetByUsername(gomock.Any(), "alex").Return(nil, domain.ErrSessionNotFound)

			result, err := svc.Start(context.Background(), domain.StartRegistrationParams{
				Email:    "alex@example.com",
				Username: "alex",
				Password: "password123",
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Status).To(Equal("conflict"))
		})

		It("creates saga state and dispatches the initial commands", func() {
			svc := newService()
			svc.mapr = mapr

			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(nil, domain.ErrSessionNotFound)
			sessions.EXPECT().GetByUsername(gomock.Any(), "alex").Return(nil, domain.ErrSessionNotFound)
			mapr.EXPECT().ToIncompleteConflictResult(nil, nil).Return(nil)
			emailChecker.EXPECT().ExistsByEmail(gomock.Any(), "alex@example.com").Return(false, nil)
			usernameChecker.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(false, nil)
			mapr.EXPECT().ToCompletedConflictResult(false, false).Return(nil)
			sessions.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(domain.Session{})).
				DoAndReturn(func(_ context.Context, session domain.Session) (*domain.Session, error) {
					return &session, nil
				})
			steps.EXPECT().Create(gomock.Any(), gomock.Any()).Times(3).Return(&domain.Step{}, nil)

			sessions.EXPECT().UpdateStatus(gomock.Any(), gomock.Any(), domain.SessionStatusInProgress).Return(nil)
			steps.EXPECT().UpdateStatus(gomock.Any(), gomock.Any(), domain.StepKeyUserCreateProfile, domain.StepStatusInProgress).Return(nil)
			steps.EXPECT().UpdateStatus(gomock.Any(), gomock.Any(), domain.StepKeyAuthCreatePending, domain.StepStatusInProgress).Return(nil)
			mapr.EXPECT().ToUserCreateCommandPayload(gomock.AssignableToTypeOf(domain.Session{}), "Alex", "Doe").Return([]byte(`{"kind":"user"}`), nil)
			broker.EXPECT().Publish(gomock.Any(), natsCfg.UserCreateSubject, []byte(`{"kind":"user"}`)).Return(nil)
			mapr.EXPECT().ToAuthCreatePendingCommandPayload(gomock.AssignableToTypeOf(domain.Session{}), "alex@example.com", gomock.Any()).Return([]byte(`{"kind":"auth"}`), nil)
			broker.EXPECT().Publish(gomock.Any(), natsCfg.AuthCreatePendingSubject, []byte(`{"kind":"auth"}`)).Return(nil)

			result, err := svc.Start(context.Background(), domain.StartRegistrationParams{
				ClientID:  "client-1",
				Email:     "alex@example.com",
				Username:  "alex",
				Password:  "password123",
				FirstName: "Alex",
				Surname:   "Doe",
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.ClientID).To(Equal("client-1"))
			Eventually(func() bool {
				time.Sleep(20 * time.Millisecond)
				return true
			}).Should(BeTrue())
		})

		It("returns step creation failures", func() {
			svc := newService()
			svc.mapr = mapr

			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(nil, domain.ErrSessionNotFound)
			sessions.EXPECT().GetByUsername(gomock.Any(), "alex").Return(nil, domain.ErrSessionNotFound)
			mapr.EXPECT().ToIncompleteConflictResult(nil, nil).Return(nil)
			emailChecker.EXPECT().ExistsByEmail(gomock.Any(), "alex@example.com").Return(false, nil)
			usernameChecker.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(false, nil)
			mapr.EXPECT().ToCompletedConflictResult(false, false).Return(nil)
			sessions.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(domain.Session{})).
				DoAndReturn(func(_ context.Context, session domain.Session) (*domain.Session, error) {
					return &session, nil
				})
			steps.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil, errors.New("step failed"))

			result, err := svc.Start(context.Background(), domain.StartRegistrationParams{
				Email:    "alex@example.com",
				Username: "alex",
				Password: "password123",
			})

			Expect(result).To(BeNil())
			Expect(err).To(MatchError("step failed"))
		})

		It("returns session creation failures", func() {
			svc := newService()
			svc.mapr = mapr

			sessions.EXPECT().GetByEmail(gomock.Any(), "alex@example.com").Return(nil, domain.ErrSessionNotFound)
			sessions.EXPECT().GetByUsername(gomock.Any(), "alex").Return(nil, domain.ErrSessionNotFound)
			mapr.EXPECT().ToIncompleteConflictResult(nil, nil).Return(nil)
			emailChecker.EXPECT().ExistsByEmail(gomock.Any(), "alex@example.com").Return(false, nil)
			usernameChecker.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(false, nil)
			mapr.EXPECT().ToCompletedConflictResult(false, false).Return(nil)
			sessions.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(domain.Session{})).
				Return(nil, errors.New("session failed"))

			result, err := svc.Start(context.Background(), domain.StartRegistrationParams{
				Email:    "alex@example.com",
				Username: "alex",
				Password: "password123",
			})

			Expect(result).To(BeNil())
			Expect(err).To(MatchError("session failed"))
		})
	})
})
