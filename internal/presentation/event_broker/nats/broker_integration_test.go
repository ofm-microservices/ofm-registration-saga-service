package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"registration-saga-service/config"
	bootstrap "registration-saga-service/pkg/messaging/nats"
)

var (
	brokerSuiteContainer testcontainers.Container
	brokerSuiteBaseCfg   config.NATSConfig
	brokerSuiteLogger    logging.Logger
)

var _ = BeforeSuite(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var err error
	brokerSuiteLogger, err = logging.New("registration-saga-service", "test", "debug")
	Expect(err).NotTo(HaveOccurred())

	brokerSuiteContainer, brokerSuiteBaseCfg = startBrokerNATSContainer(ctx)
})

var _ = AfterSuite(func() {
	if brokerSuiteContainer != nil {
		Expect(brokerSuiteContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("natsBroker integration", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		cfg    config.NATSConfig
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 45*time.Second)
		cfg = uniqueBrokerConfig(brokerSuiteBaseCfg)
		Expect(ensureBrokerStreams(cfg)).To(Succeed())
	})

	AfterEach(func() {
		cancel()
	})

	It("validates broker construction", func() {
		broker, err := NewBroker(config.NATSConfig{}, brokerSuiteLogger)
		Expect(broker).To(BeNil())
		Expect(err).To(MatchError(ErrEmptyNATSURL))

		broker, err = NewBroker(cfg, nil)
		Expect(broker).To(BeNil())
		Expect(err).To(MatchError(ErrNilLogger))
	})

	It("publishes messages to core nats subjects", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		sub, err := rawConn.SubscribeSync(cfg.RegistrationCodeSentSubject)
		Expect(err).NotTo(HaveOccurred())
		Expect(rawConn.Flush()).To(Succeed())

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		defer brokerAny.Close()

		Expect(brokerAny.Publish(ctx, cfg.RegistrationCodeSentSubject, []byte("payload"))).To(Succeed())

		msg, err := sub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(msg.Data)).To(Equal("payload"))
	})

	It("flushes with a synthetic timeout when the caller provides no deadline", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		Expect(Flush(context.Background(), rawConn)).To(Succeed())
	})

	It("subscribes and dispatches core nats messages", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		defer brokerAny.Close()

		received := make(chan []byte, 1)
		Expect(brokerAny.Subscribe(ctx, cfg.RegistrationCodeSentSubject, func(_ context.Context, subject string, payload []byte) error {
			Expect(subject).To(Equal(cfg.RegistrationCodeSentSubject))
			received <- payload
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.RegistrationCodeSentSubject, []byte("payload"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(received).Should(Receive(Equal([]byte("payload"))))
	})

	It("continues after a subscriber handler error", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		defer brokerAny.Close()

		received := make(chan string, 1)
		Expect(brokerAny.Subscribe(ctx, cfg.RegistrationCodeSentSubject, func(_ context.Context, _ string, payload []byte) error {
			if string(payload) == "bad" {
				return errors.New("boom")
			}
			received <- string(payload)
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.RegistrationCodeSentSubject, []byte("bad"))).To(Succeed())
		Expect(rawConn.Publish(cfg.RegistrationCodeSentSubject, []byte("good"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(received).Should(Receive(Equal("good")))
	})

	It("runs a pull consumer against jetstream", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		var handled atomic.Int32
		Expect(concrete.RunPullConsumer(runCtx, config.PullConsumerConfig{
			Stream:     cfg.UserEventsStream,
			Subject:    cfg.UserCreateResultSubject,
			Durable:    cfg.UserCreateResultDurable,
			BatchSize:  2,
			MaxWait:    50 * time.Millisecond,
			Workers:    1,
			QueueSize:  4,
			AckWait:    2 * time.Second,
			MaxDeliver: 3,
			Adaptive: config.PullAdaptiveConfig{
				Enabled:         true,
				CheckInterval:   time.Millisecond,
				MediumPending:   1,
				HighPending:     2,
				LowBatchSize:    1,
				LowMaxWait:      20 * time.Millisecond,
				MediumBatchSize: 2,
				MediumMaxWait:   10 * time.Millisecond,
				HighBatchSize:   3,
				HighMaxWait:     5 * time.Millisecond,
			},
		}, func(_ context.Context, subject string, payload []byte) error {
			Expect(subject).To(Equal(cfg.UserCreateResultSubject))
			Expect(payload).NotTo(BeEmpty())
			handled.Add(1)
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.UserCreateResultSubject, []byte("one"))).To(Succeed())
		Expect(rawConn.Publish(cfg.UserCreateResultSubject, []byte("two"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(func() int32 { return handled.Load() }).Should(BeNumerically(">=", 2))
		runCancel()
	})

	It("falls back to default validator and runtime factory when broker helpers are nil", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		concrete.validator = nil
		concrete.runtimeFactory = nil
		defer concrete.Close()

		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		var handled atomic.Int32
		Expect(concrete.RunPullConsumer(runCtx, config.PullConsumerConfig{
			Stream:     cfg.UserEventsStream,
			Subject:    cfg.UserCreateResultSubject,
			Durable:    cfg.UserCreateResultDurable,
			BatchSize:  1,
			MaxWait:    50 * time.Millisecond,
			Workers:    1,
			QueueSize:  2,
			AckWait:    time.Second,
			MaxDeliver: 2,
		}, func(_ context.Context, _ string, _ []byte) error {
			handled.Add(1)
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.UserCreateResultSubject, []byte("one"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(func() int32 { return handled.Load() }).Should(Equal(int32(1)))
	})

	It("validates pull consumer config before touching nats", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		err = concrete.RunPullConsumer(ctx, config.PullConsumerConfig{}, func(context.Context, string, []byte) error { return nil })
		Expect(err).To(MatchError(ErrEmptyStreamName))

		err = concrete.RunPullConsumer(ctx, config.PullConsumerConfig{Stream: "stream"}, func(context.Context, string, []byte) error { return nil })
		Expect(err).To(MatchError(ErrEmptySubject))
	})

	It("validates the remaining pull-consumer config branches", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		base := config.PullConsumerConfig{
			Stream:     "stream",
			Subject:    "subject",
			Durable:    "durable",
			BatchSize:  1,
			MaxWait:    time.Millisecond,
			Workers:    1,
			QueueSize:  1,
			AckWait:    time.Second,
			MaxDeliver: 1,
		}

		cfgBad := base
		cfgBad.Durable = ""
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrEmptyDurableName))

		cfgBad = base
		cfgBad.BatchSize = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidBatchSize))

		cfgBad = base
		cfgBad.MaxWait = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidMaxWait))

		cfgBad = base
		cfgBad.Workers = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidWorkerCount))

		cfgBad = base
		cfgBad.QueueSize = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidQueueSize))

		cfgBad = base
		cfgBad.AckWait = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAckWait))

		cfgBad = base
		cfgBad.MaxDeliver = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidMaxDeliver))

		cfgBad = base
		cfgBad.Adaptive = config.PullAdaptiveConfig{Enabled: true}
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAdaptiveCheckInterval))

		cfgBad = base
		cfgBad.Adaptive = config.PullAdaptiveConfig{
			Enabled:       true,
			CheckInterval: time.Millisecond,
			MediumPending: 2,
			HighPending:   2,
		}
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAdaptiveThresholds))

		cfgBad = base
		cfgBad.Adaptive = config.PullAdaptiveConfig{
			Enabled:         true,
			CheckInterval:   time.Millisecond,
			MediumPending:   1,
			HighPending:     2,
			LowBatchSize:    0,
			LowMaxWait:      time.Millisecond,
			MediumBatchSize: 1,
			MediumMaxWait:   time.Millisecond,
			HighBatchSize:   1,
			HighMaxWait:     time.Millisecond,
		}
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAdaptivePlan))
	})

	It("wraps publish and subscribe failures on a closed connection", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		concrete.Close()

		err = concrete.Publish(ctx, cfg.RegistrationCodeSentSubject, []byte("payload"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("publish to nats"))

		err = concrete.Subscribe(ctx, cfg.RegistrationCodeSentSubject, func(context.Context, string, []byte) error { return nil })
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("subscribe to nats"))
	})

	It("naks and redelivers when the pull-consumer handler fails", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		var handled atomic.Int32
		Expect(concrete.RunPullConsumer(runCtx, config.PullConsumerConfig{
			Stream:     cfg.AuthEventsStream,
			Subject:    cfg.AuthCreatePendingResultSubject,
			Durable:    cfg.AuthCreatePendingResultDurable,
			BatchSize:  1,
			MaxWait:    20 * time.Millisecond,
			Workers:    1,
			QueueSize:  2,
			AckWait:    time.Second,
			MaxDeliver: 3,
		}, func(_ context.Context, _ string, _ []byte) error {
			if handled.Add(1) == 1 {
				return errors.New("retry")
			}
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.AuthCreatePendingResultSubject, []byte("retry-me"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(func() int32 { return handled.Load() }).Should(BeNumerically(">=", 2))
	})

	It("closes safely", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())

		brokerAny.Close()
		brokerAny.Close()
	})
})

var _ = Describe("pullConsumerRuntime helpers", func() {
	var (
		cancel context.CancelFunc
		cfg    config.NATSConfig
	)

	BeforeEach(func() {
		_, cancel = context.WithTimeout(context.Background(), 45*time.Second)
		cfg = uniqueBrokerConfig(brokerSuiteBaseCfg)
		Expect(ensureBrokerStreams(cfg)).To(Succeed())
	})

	AfterEach(func() {
		cancel()
	})

	It("updates the adaptive plan from pending consumer info", func() {
		nc, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer nc.Close()

		factory := newPullConsumerRuntimeFactory()
		runtimeAny, err := factory.Create(nc, brokerSuiteLogger, config.PullConsumerConfig{
			Stream:     cfg.UserEventsStream,
			Subject:    cfg.UserCreateResultSubject,
			Durable:    cfg.UserCreateResultDurable,
			BatchSize:  1,
			MaxWait:    time.Second,
			Workers:    1,
			QueueSize:  2,
			AckWait:    time.Second,
			MaxDeliver: 3,
			Adaptive: config.PullAdaptiveConfig{
				Enabled:         true,
				CheckInterval:   time.Millisecond,
				MediumPending:   1,
				HighPending:     2,
				LowBatchSize:    1,
				LowMaxWait:      time.Second,
				MediumBatchSize: 2,
				MediumMaxWait:   500 * time.Millisecond,
				HighBatchSize:   3,
				HighMaxWait:     100 * time.Millisecond,
			},
		}, func(context.Context, string, []byte) error { return nil })
		Expect(err).NotTo(HaveOccurred())

		runtime := runtimeAny.(*pullConsumerRuntime)
		Expect(nc.Publish(cfg.UserCreateResultSubject, []byte("one"))).To(Succeed())
		Expect(nc.Publish(cfg.UserCreateResultSubject, []byte("two"))).To(Succeed())
		Expect(nc.Flush()).To(Succeed())

		state := pullConsumerFetchState{
			batch:             1,
			wait:              time.Second,
			tier:              "base",
			lastAdaptiveCheck: time.Now().Add(-time.Second),
		}

		Eventually(func() string {
			runtime.maybeUpdateAdaptivePlan(&state)
			return state.tier
		}).Should(Equal("high"))
		Expect(state.batch).To(Equal(3))
		Expect(state.wait).To(Equal(100 * time.Millisecond))
	})

	It("covers the runtime helper branches", func() {
		runtime := &pullConsumerRuntime{
			log:  brokerSuiteLogger,
			cfg:  config.PullConsumerConfig{Subject: "subject", Durable: "durable", Adaptive: config.PullAdaptiveConfig{Enabled: true, CheckInterval: time.Second}},
			jobs: make(chan *nats.Msg, 1),
		}

		Expect(runtime.shouldStop(context.Background())).To(BeFalse())

		stopCtx, cancel := context.WithCancel(context.Background())
		cancel()
		Expect(runtime.shouldStop(stopCtx)).To(BeTrue())

		state := pullConsumerFetchState{
			batch:             1,
			wait:              time.Second,
			tier:              "base",
			lastAdaptiveCheck: time.Now(),
		}
		runtime.maybeUpdateAdaptivePlan(&state)
		Expect(state.tier).To(Equal("base"))

		Expect(runtime.handleFetchError(nil)).To(BeFalse())
		Expect(runtime.handleFetchError(nats.ErrTimeout)).To(BeTrue())
		Expect(runtime.handleFetchError(context.DeadlineExceeded)).To(BeTrue())
		Expect(runtime.handleFetchError(errors.New("boom"))).To(BeTrue())

		msg := &nats.Msg{Subject: "subject", Data: []byte("payload")}
		Expect(runtime.dispatchFetchedMessages(context.Background(), []*nats.Msg{msg})).To(BeFalse())
		Expect(<-runtime.jobs).To(Equal(msg))

		blockedRuntime := &pullConsumerRuntime{jobs: make(chan *nats.Msg, 1)}
		blockedRuntime.jobs <- &nats.Msg{}
		canceled, cancelDispatch := context.WithCancel(context.Background())
		cancelDispatch()
		Expect(blockedRuntime.dispatchFetchedMessages(canceled, []*nats.Msg{{}})).To(BeTrue())
	})
})

func startBrokerNATSContainer(ctx context.Context) (testcontainers.Container, config.NATSConfig) {
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
		UserCreateResultDurable:        "registration_saga_user_create_result",
		AuthCreatePendingResultDurable: "registration_saga_auth_create_pending_result",
		MailSendResultDurable:          "registration_saga_mail_send_result",
	}
}

func ensureBrokerStreams(cfg config.NATSConfig) error {
	return bootstrap.EnsureStream(cfg, brokerSuiteLogger)
}

func uniqueBrokerConfig(base config.NATSConfig) config.NATSConfig {
	suffix := time.Now().UTC().Format("150405000000000")
	cfg := base
	cfg.RegistrationEventsStream += "_" + suffix
	cfg.UserEventsStream += "_" + suffix
	cfg.AuthEventsStream += "_" + suffix
	cfg.MailEventsStream += "_" + suffix
	cfg.RegistrationCodeSentSubject += "." + suffix
	cfg.RegistrationCompletedSubject += "." + suffix
	cfg.RegistrationFailedSubject += "." + suffix
	cfg.UserCreateSubject += "." + suffix
	cfg.UserCreateResultSubject += "." + suffix
	cfg.AuthCreatePendingSubject += "." + suffix
	cfg.AuthCreatePendingResultSubject += "." + suffix
	cfg.MailSendResultSubject += "." + suffix
	cfg.UserCreateResultDurable += "_" + suffix
	cfg.AuthCreatePendingResultDurable += "_" + suffix
	cfg.MailSendResultDurable += "_" + suffix
	return cfg
}
