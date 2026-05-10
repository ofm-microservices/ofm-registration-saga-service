package appfx

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/fx/fxtest"
	"go.uber.org/mock/gomock"
	"registration-saga-service/config"
	app "registration-saga-service/internal/application"
	domain "registration-saga-service/internal/domain"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
	events "registration-saga-service/internal/presentation/event_broker/nats"
	grpcserver "registration-saga-service/internal/presentation/grpc"
)

func TestFX(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "FX Suite")
}

type stubSessionRepo struct{}

func (stubSessionRepo) Create(context.Context, domain.Session) (*domain.Session, error) {
	return &domain.Session{}, nil
}
func (stubSessionRepo) GetByID(context.Context, string) (*domain.Session, error)    { return nil, nil }
func (stubSessionRepo) GetByEmail(context.Context, string) (*domain.Session, error) { return nil, nil }
func (stubSessionRepo) GetByUsername(context.Context, string) (*domain.Session, error) {
	return nil, nil
}
func (stubSessionRepo) UpdateStatus(context.Context, string, string) error   { return nil }
func (stubSessionRepo) ClaimCompleted(context.Context, string) (bool, error) { return false, nil }

type stubStepRepo struct{}

func (stubStepRepo) Create(context.Context, domain.Step) (*domain.Step, error) {
	return &domain.Step{}, nil
}
func (stubStepRepo) GetByKey(context.Context, string, string) (*domain.Step, error) { return nil, nil }
func (stubStepRepo) ListBySessionID(context.Context, string) ([]domain.Step, error) { return nil, nil }
func (stubStepRepo) UpdateStatus(context.Context, string, string, string) error     { return nil }

type stubEmailChecker struct{}

func (stubEmailChecker) ExistsByEmail(context.Context, string) (bool, error) { return false, nil }
func (stubEmailChecker) VerifyRegistrationEmail(context.Context, string, string) error {
	return nil
}
func (stubEmailChecker) DeactivateRegistrationAuth(context.Context, string) error { return nil }

type stubUsernameChecker struct{}

func (stubUsernameChecker) ExistsByUsername(context.Context, string) (bool, error) { return false, nil }
func (stubUsernameChecker) ActivateUser(context.Context, string) error             { return nil }
func (stubUsernameChecker) DeactivateUser(context.Context, string) error           { return nil }

type stubRegistrationService struct{}

func (stubRegistrationService) Start(context.Context, domain.StartRegistrationParams) (*domain.StartRegistrationResult, error) {
	return &domain.StartRegistrationResult{}, nil
}
func (stubRegistrationService) VerifyEmail(context.Context, domain.VerifyEmailParams) (*domain.VerifyEmailResult, error) {
	return &domain.VerifyEmailResult{}, nil
}
func (stubRegistrationService) GetRegistrationStatus(context.Context, string, string) (*domain.RegistrationStatus, error) {
	return &domain.RegistrationStatus{}, nil
}
func (stubRegistrationService) HandleUserCreateResult(context.Context, app.UserCreateResult) error {
	return nil
}
func (stubRegistrationService) HandleAuthCreatePendingResult(context.Context, app.AuthCreatePendingResult) error {
	return nil
}
func (stubRegistrationService) HandleMailSendResult(context.Context, app.MailSendResult) error {
	return nil
}

type resultSubscriberStub struct {
	calls int
	err   error
}

func (s *resultSubscriberStub) Subscribe(context.Context) error {
	s.calls++
	return s.err
}

var _ = Describe("fx providers and invokes", func() {
	var (
		ctrl   *gomock.Controller
		logger logging.Logger
		cfg    *config.Config
		lc     *fxtest.Lifecycle
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())

		var err error
		logger, err = logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())

		cfg = &config.Config{
			App: config.AppConfig{
				Env:      "test",
				LogLevel: "debug",
			},
			GRPC: config.GRPCConfig{
				Host: "127.0.0.1",
				Port: 19095,
			},
			NATS: config.NATSConfig{
				URL:                            "nats://127.0.0.1:4222",
				RegistrationEventsStream:       "REGISTRATION_EVENTS",
				UserEventsStream:               "USER_EVENTS",
				AuthEventsStream:               "AUTH_EVENTS",
				MailEventsStream:               "MAIL_EVENTS",
				RegistrationCodeSentSubject:    "registration.code.sent",
				RegistrationCompletedSubject:   "registration.completed",
				RegistrationFailedSubject:      "registration.failed",
				UserCreateSubject:              "saga.user.create",
				UserCreateResultSubject:        "saga.user.create.result",
				AuthCreatePendingSubject:       "saga.auth.create_pending_registration",
				AuthCreatePendingResultSubject: "saga.auth.create_pending_registration.result",
				MailSendResultSubject:          "mail.send.result",
			},
			AuthService: config.AuthServiceConfig{Address: "127.0.0.1:9501"},
			UserService: config.UserServiceConfig{Address: "127.0.0.1:9502"},
		}

		lc = fxtest.NewLifecycle(GinkgoT())
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	It("logs startup", func() {
		InvokeStartLog(logger)
	})

	It("provides config from environment", func() {
		env := map[string]string{
			"NATS_URL":             "nats://127.0.0.1:4222",
			"SCYLLA_HOSTS":         "127.0.0.1",
			"AUTH_SERVICE_ADDRESS": "127.0.0.1:9501",
			"USER_SERVICE_ADDRESS": "127.0.0.1:9502",
		}

		for key, value := range env {
			original, exists := os.LookupEnv(key)
			key, value, original, exists := key, value, original, exists
			DeferCleanup(func() {
				if exists {
					Expect(os.Setenv(key, original)).To(Succeed())
					return
				}
				Expect(os.Unsetenv(key)).To(Succeed())
			})
			Expect(os.Setenv(key, value)).To(Succeed())
		}

		loaded, err := ProvideConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.NATS.URL).To(Equal("nats://127.0.0.1:4222"))
		Expect(loaded.AuthService.Address).To(Equal("127.0.0.1:9501"))
	})

	It("provides a logger", func() {
		provided, err := ProvideLogger(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(provided).NotTo(BeNil())
	})

	It("fails logger construction on invalid log levels", func() {
		badCfg := *cfg
		badCfg.App.LogLevel = "definitely-invalid"

		provided, err := ProvideLogger(&badCfg)
		Expect(provided).To(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("constructs the application service", func() {
		svc, err := ProvideRegistrationService(
			stubSessionRepo{},
			stubStepRepo{},
			&eventBrokerStub{},
			stubEmailChecker{},
			stubUsernameChecker{},
			cfg,
			logger,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(svc).NotTo(BeNil())
	})

	It("propagates repository constructor validation", func() {
		repo, err := ProvideSessionRepository(nil)
		Expect(repo).To(BeNil())
		Expect(err).To(MatchError("scylla session is nil"))

		stepRepo, err := ProvideStepRepository(nil)
		Expect(stepRepo).To(BeNil())
		Expect(err).To(MatchError("scylla session is nil"))
	})

	It("constructs presentation adapters", func() {
		service := stubRegistrationService{}
		broker := &eventBrokerStub{}

		subscriber, err := ProvideResultSubscriber(broker, service, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(subscriber).NotTo(BeNil())

		server, err := ProvideGRPCServer(service, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(server).NotTo(BeNil())
	})

	It("constructs outbound availability clients and appends stop hooks", func() {
		emailChecker, err := ProvideAuthAvailabilityChecker(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(emailChecker).NotTo(BeNil())

		usernameChecker, err := ProvideUserAvailabilityChecker(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(usernameChecker).NotTo(BeNil())

		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("propagates nats bootstrap and broker construction failures", func() {
		badCfg := *cfg
		badCfg.NATS.URL = "nats://127.0.0.1:1"

		Expect(InvokeEnsureStream(&badCfg, logger)).To(HaveOccurred())

		badCfg.NATS.URL = ""
		eventBroker, err := ProvideEventBroker(lc, &badCfg, logger)
		Expect(eventBroker).To(BeNil())
		Expect(err).To(MatchError("nats url is empty"))
	})

	It("propagates scylla open failures", func() {
		badCfg := *cfg
		badCfg.Scylla = config.ScyllaConfig{
			Hosts:                  []string{"127.0.0.1"},
			Port:                   1,
			Keyspace:               "registration_saga_test",
			Consistency:            "quorum",
			ConnectTimeout:         time.Millisecond,
			MaxWaitSchemaAgreement: time.Millisecond,
			RetryAttempts:          1,
			RetryBackoff:           time.Millisecond,
		}

		session, err := ProvideScyllaSession(lc, &badCfg, logger)
		Expect(session).To(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("provides a live event broker and bootstraps streams", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		container, natsCfg := startFXNATSContainer(ctx)
		defer func() {
			Expect(container.Terminate(context.Background())).To(Succeed())
		}()

		goodCfg := *cfg
		goodCfg.NATS = natsCfg

		Expect(InvokeEnsureStream(&goodCfg, logger)).To(Succeed())

		eventBroker, err := ProvideEventBroker(lc, &goodCfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(eventBroker).NotTo(BeNil())

		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("provides a live scylla session and closes it on stop", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		container, scyllaCfg := startFXScyllaContainer(ctx, "registration_saga_fx")
		defer func() {
			Expect(container.Terminate(context.Background())).To(Succeed())
		}()

		goodCfg := *cfg
		goodCfg.Scylla = scyllaCfg

		session, err := ProvideScyllaSession(lc, &goodCfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(session).NotTo(BeNil())

		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("registers subscriber lifecycle hooks and runs start-stop cleanly", func() {
		subscriber := &resultSubscriberStub{}

		InvokeSubscribeResults(lc, subscriber, cfg, logger)

		Expect(lc.Start(context.Background())).To(Succeed())
		Expect(lc.Stop(context.Background())).To(Succeed())
		Expect(subscriber.calls).To(Equal(1))
	})

	It("propagates subscriber startup failures", func() {
		subscriber := &resultSubscriberStub{err: errors.New("boom")}

		InvokeSubscribeResults(lc, subscriber, cfg, logger)

		Expect(lc.Start(context.Background())).To(MatchError("boom"))
		Expect(subscriber.calls).To(Equal(1))
	})

	It("registers grpc lifecycle hooks and stops the server", func() {
		server := &stubGRPCServer{}
		started := make(chan struct{}, 1)

		server.start = func() error {
			started <- struct{}{}
			return nil
		}
		server.shutdown = func(context.Context) error { return nil }

		InvokeRunGRPCServer(lc, server)

		Expect(lc.Start(context.Background())).To(Succeed())
		Eventually(started).Should(Receive())
		Expect(lc.Stop(context.Background())).To(Succeed())
	})
})

type eventBrokerStub struct{}

func (eventBrokerStub) Publish(context.Context, string, []byte) error { return nil }
func (eventBrokerStub) Subscribe(context.Context, string, eventbroker.MessageHandler) error {
	return nil
}
func (eventBrokerStub) RunPullConsumer(context.Context, config.PullConsumerConfig, eventbroker.MessageHandler) error {
	return nil
}
func (eventBrokerStub) Close() {}

type stubGRPCServer struct {
	start    func() error
	shutdown func(context.Context) error
}

func (s *stubGRPCServer) Start() error {
	if s.start != nil {
		return s.start()
	}
	return nil
}

func (s *stubGRPCServer) Shutdown(ctx context.Context) error {
	if s.shutdown != nil {
		return s.shutdown(ctx)
	}
	return nil
}

var _ events.ResultSubscriber = (*resultSubscriberStub)(nil)
var _ grpcserver.Server = (*stubGRPCServer)(nil)

func startFXNATSContainer(ctx context.Context) (testcontainers.Container, config.NATSConfig) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.12.4-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js"},
			WaitingFor:   wait.ForListeningPort("4222/tcp"),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred())

	host, err := container.Host(ctx)
	Expect(err).NotTo(HaveOccurred())
	port, err := container.MappedPort(ctx, "4222/tcp")
	Expect(err).NotTo(HaveOccurred())

	return container, config.NATSConfig{
		URL:                            "nats://" + host + ":" + port.Port(),
		RegistrationEventsStream:       "REGISTRATION_EVENTS",
		UserEventsStream:               "USER_EVENTS",
		AuthEventsStream:               "AUTH_EVENTS",
		MailEventsStream:               "MAIL_EVENTS",
		RegistrationCodeSentSubject:    "registration.code.sent",
		RegistrationCompletedSubject:   "registration.completed",
		RegistrationFailedSubject:      "registration.failed",
		UserCreateSubject:              "saga.user.create",
		UserCreateResultSubject:        "saga.user.create.result",
		AuthCreatePendingSubject:       "saga.auth.create_pending_registration",
		AuthCreatePendingResultSubject: "saga.auth.create_pending_registration.result",
		MailSendResultSubject:          "mail.send.result",
	}
}

func startFXScyllaContainer(ctx context.Context, keyspace string) (testcontainers.Container, config.ScyllaConfig) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "scylladb/scylla:6.1",
			ExposedPorts: []string{"9042/tcp"},
			Cmd:          []string{"--smp", "1", "--memory", "512M", "--overprovisioned", "1"},
			WaitingFor:   wait.ForListeningPort("9042/tcp").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred())

	host, err := container.Host(ctx)
	Expect(err).NotTo(HaveOccurred())
	port, err := container.MappedPort(ctx, "9042/tcp")
	Expect(err).NotTo(HaveOccurred())
	portNum, err := strconv.Atoi(port.Port())
	Expect(err).NotTo(HaveOccurred())

	return container, config.ScyllaConfig{
		Hosts:                  []string{host},
		Port:                   portNum,
		Keyspace:               keyspace,
		Consistency:            "quorum",
		ConnectTimeout:         10 * time.Second,
		MaxWaitSchemaAgreement: 30 * time.Second,
		RetryAttempts:          30,
		RetryBackoff:           2 * time.Second,
	}
}
