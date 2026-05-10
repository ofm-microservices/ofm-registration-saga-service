package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"registration-saga-service/config"
	app "registration-saga-service/internal/application"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
)

func TestNATS(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "NATS Suite")
}

var _ = Describe("NATS presentation", func() {
	var logger logging.Logger

	BeforeEach(func() {
		var err error
		logger, err = logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("errors", func() {
		It("wraps broker failures with context", func() {
			err := errors.New("boom")

			Expect(WrapConnectToNATSError(err)).To(MatchError(ContainSubstring("connect to nats")))
			Expect(WrapPublishToNATSError("subject", err)).To(MatchError(ContainSubstring("publish to nats (subject)")))
			Expect(WrapSubscribeToNATSError("subject", err)).To(MatchError(ContainSubstring("subscribe to nats (subject)")))
			Expect(WrapFlushNATSPublisherError(err)).To(MatchError(ContainSubstring("flush nats publisher")))
			Expect(WrapInitJetStreamContextError(err)).To(MatchError(ContainSubstring("init jetstream context")))
			Expect(WrapEnsureConsumerError("stream", "durable", err, err)).To(MatchError(ContainSubstring(`ensure consumer "durable" in stream "stream"`)))
			Expect(WrapCreatePullSubscriberError("subject", "durable", err)).To(MatchError(ContainSubstring(`create pull subscriber subject="subject" durable="durable"`)))
			Expect(WrapUnmarshalUserCreateResultError(err)).To(MatchError(ContainSubstring("unmarshal user create result")))
			Expect(WrapUnmarshalAuthCreatePendingResultError(err)).To(MatchError(ContainSubstring("unmarshal auth create pending registration result")))
			Expect(WrapUnmarshalMailSendResultError(err)).To(MatchError(ContainSubstring("unmarshal mail send result")))
		})
	})

	Describe("pull consumer validator", func() {
		baseCfg := func() config.PullConsumerConfig {
			return config.PullConsumerConfig{
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
		}

		It("validates required fields and adaptive thresholds", func() {
			v := newPullConsumerConfigValidator()

			cfg := baseCfg()
			cfg.Stream = ""
			Expect(v.Validate(cfg)).To(MatchError(ErrEmptyStreamName))

			cfg = baseCfg()
			cfg.Subject = ""
			Expect(v.Validate(cfg)).To(MatchError(ErrEmptySubject))

			cfg = baseCfg()
			cfg.Durable = ""
			Expect(v.Validate(cfg)).To(MatchError(ErrEmptyDurableName))

			cfg = baseCfg()
			cfg.BatchSize = 0
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidBatchSize))

			cfg = baseCfg()
			cfg.MaxWait = 0
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidMaxWait))

			cfg = baseCfg()
			cfg.Workers = 0
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidWorkerCount))

			cfg = baseCfg()
			cfg.QueueSize = 0
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidQueueSize))

			cfg = baseCfg()
			cfg.AckWait = 0
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidAckWait))

			cfg = baseCfg()
			cfg.MaxDeliver = 0
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidMaxDeliver))

			cfg = baseCfg()
			cfg.Adaptive = config.PullAdaptiveConfig{Enabled: true}
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidAdaptiveCheckInterval))

			cfg = baseCfg()
			cfg.Adaptive = config.PullAdaptiveConfig{
				Enabled:       true,
				CheckInterval: time.Second,
				MediumPending: 10,
				HighPending:   10,
			}
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidAdaptiveThresholds))

			cfg = baseCfg()
			cfg.Adaptive = config.PullAdaptiveConfig{
				Enabled:         true,
				CheckInterval:   time.Second,
				MediumPending:   10,
				HighPending:     20,
				LowBatchSize:    1,
				LowMaxWait:      time.Millisecond,
				MediumBatchSize: 0,
				MediumMaxWait:   time.Millisecond,
				HighBatchSize:   1,
				HighMaxWait:     time.Millisecond,
			}
			Expect(v.Validate(cfg)).To(MatchError(ErrInvalidAdaptivePlan))

			cfg = baseCfg()
			cfg.Adaptive = config.PullAdaptiveConfig{
				Enabled:         true,
				CheckInterval:   time.Second,
				MediumPending:   10,
				HighPending:     20,
				LowBatchSize:    1,
				LowMaxWait:      time.Millisecond,
				MediumBatchSize: 2,
				MediumMaxWait:   time.Millisecond,
				HighBatchSize:   3,
				HighMaxWait:     time.Millisecond,
			}
			Expect(v.Validate(cfg)).To(Succeed())
		})
	})

	Describe("result subscriber", func() {
		var (
			ctrl    *gomock.Controller
			broker  *MockEventBroker
			service *MockRegistrationService
			cfg     config.NATSConfig
		)

		BeforeEach(func() {
			ctrl = gomock.NewController(GinkgoT())
			broker = NewMockEventBroker(ctrl)
			service = NewMockRegistrationService(ctrl)
			cfg = config.NATSConfig{
				UserEventsStream:               "USER_EVENTS",
				UserCreateResultSubject:        "saga.user.create.result",
				UserCreateResultDurable:        "user-durable",
				AuthEventsStream:               "AUTH_EVENTS",
				AuthCreatePendingResultSubject: "saga.auth.create_pending.result",
				AuthCreatePendingResultDurable: "auth-durable",
				MailEventsStream:               "MAIL_EVENTS",
				MailSendResultSubject:          "mail.send.result",
				MailSendResultDurable:          "mail-durable",
				ResultBatchSize:                5,
				ResultMaxWait:                  time.Millisecond,
				ResultWorkers:                  2,
				ResultQueueSize:                8,
				ResultAckWait:                  time.Second,
				ResultMaxDeliver:               3,
				ResultAdaptiveEnabled:          true,
				ResultAdaptiveCheckInterval:    time.Second,
				ResultAdaptiveMediumPending:    10,
				ResultAdaptiveHighPending:      20,
				ResultAdaptiveLowBatchSize:     1,
				ResultAdaptiveLowMaxWait:       time.Millisecond,
				ResultAdaptiveMediumBatchSize:  2,
				ResultAdaptiveMediumMaxWait:    2 * time.Millisecond,
				ResultAdaptiveHighBatchSize:    3,
				ResultAdaptiveHighMaxWait:      3 * time.Millisecond,
			}
		})

		AfterEach(func() {
			ctrl.Finish()
		})

		It("validates nil collaborators", func() {
			sub, err := NewResultSubscriber(nil, service, cfg, logger)
			Expect(sub).To(BeNil())
			Expect(err).To(MatchError(ErrNilBroker))

			sub, err = NewResultSubscriber(broker, nil, cfg, logger)
			Expect(sub).To(BeNil())
			Expect(err).To(MatchError(ErrNilRegistrationService))

			sub, err = NewResultSubscriber(broker, service, cfg, nil)
			Expect(sub).To(BeNil())
			Expect(err).To(MatchError(ErrNilLogger))
		})

		It("subscribes all result handlers and forwards decoded payloads", func() {
			sub, err := NewResultSubscriber(broker, service, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			gomock.InOrder(
				broker.EXPECT().
					RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, pullCfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
						Expect(pullCfg.Stream).To(Equal(cfg.UserEventsStream))
						service.EXPECT().HandleUserCreateResult(gomock.Any(), app.UserCreateResult{
							SessionID: "session-1",
							UserID:    "user-1",
							Status:    "success",
							Timestamp: "ts",
						}).Return(nil)
						return handler(ctx, "", []byte(`{"session_id":"session-1","user_id":"user-1","status":"success","timestamp":"ts"}`))
					}),
				broker.EXPECT().
					RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, pullCfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
						Expect(pullCfg.Stream).To(Equal(cfg.AuthEventsStream))
						service.EXPECT().HandleAuthCreatePendingResult(gomock.Any(), app.AuthCreatePendingResult{
							SessionID: "session-1",
							ClientID:  "client-1",
							UserID:    "user-1",
							Status:    "success",
							Timestamp: "ts",
						}).Return(nil)
						return handler(ctx, "", []byte(`{"session_id":"session-1","client_id":"client-1","user_id":"user-1","status":"success","timestamp":"ts"}`))
					}),
				broker.EXPECT().
					RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, pullCfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
						Expect(pullCfg.Stream).To(Equal(cfg.MailEventsStream))
						service.EXPECT().HandleMailSendResult(gomock.Any(), app.MailSendResult{
							SessionID: "session-1",
							ClientID:  "client-1",
							UserID:    "user-1",
							Status:    "success",
							Timestamp: "ts",
						}).Return(nil)
						return handler(ctx, "", []byte(`{"session_id":"session-1","client_id":"client-1","user_id":"user-1","status":"success","timestamp":"ts"}`))
					}),
			)

			Expect(sub.Subscribe(context.Background())).To(Succeed())
		})

		It("returns decode failures from handlers", func() {
			sub, err := NewResultSubscriber(broker, service, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, _ config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					return handler(ctx, "", []byte(`{`))
				})

			Expect(sub.Subscribe(context.Background())).To(MatchError(ContainSubstring("unmarshal user create result")))
		})
	})

	Describe("broker runtime orchestration", func() {
		It("validates config, creates runtime, and starts it", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			validator := NewMockPullConsumerConfigValidator(ctrl)
			factory := NewMockPullConsumerRuntimeFactory(ctrl)
			runtime := NewMockPullConsumerRuntime(ctrl)
			b := &natsBroker{
				log:            logger,
				validator:      validator,
				runtimeFactory: factory,
			}
			cfg := config.PullConsumerConfig{Stream: "stream"}

			validator.EXPECT().Validate(cfg).Return(nil)
			factory.EXPECT().Create(nil, gomock.Any(), cfg, gomock.Any()).Return(runtime, nil)
			runtime.EXPECT().Start(gomock.Any())

			Expect(b.RunPullConsumer(context.Background(), cfg, func(context.Context, string, []byte) error { return nil })).To(Succeed())
		})

		It("resolves adaptive pull plans", func() {
			cfg := config.PullConsumerConfig{
				BatchSize: 10,
				MaxWait:   10 * time.Millisecond,
				Adaptive: config.PullAdaptiveConfig{
					Enabled:         true,
					MediumPending:   50,
					HighPending:     100,
					LowBatchSize:    5,
					LowMaxWait:      25 * time.Millisecond,
					MediumBatchSize: 10,
					MediumMaxWait:   10 * time.Millisecond,
					HighBatchSize:   20,
					HighMaxWait:     2 * time.Millisecond,
				},
			}

			tier, batch, wait := ResolvePullPlan(cfg, 100)
			Expect(tier).To(Equal("high"))
			Expect(batch).To(Equal(20))
			Expect(wait).To(Equal(2 * time.Millisecond))

			tier, batch, wait = ResolvePullPlan(cfg, 60)
			Expect(tier).To(Equal("medium"))
			Expect(batch).To(Equal(10))
			Expect(wait).To(Equal(10 * time.Millisecond))

			tier, batch, wait = ResolvePullPlan(cfg, 10)
			Expect(tier).To(Equal("low"))
			Expect(batch).To(Equal(5))
			Expect(wait).To(Equal(25 * time.Millisecond))
		})

		It("handles fetch errors", func() {
			runtime := &pullConsumerRuntime{
				log: logger,
				cfg: config.PullConsumerConfig{Subject: "subject", Durable: "durable"},
			}

			Expect(runtime.handleFetchError(nil)).To(BeFalse())
			Expect(runtime.handleFetchError(nats.ErrTimeout)).To(BeTrue())
			Expect(runtime.handleFetchError(context.DeadlineExceeded)).To(BeTrue())
			Expect(runtime.handleFetchError(errors.New("boom"))).To(BeTrue())
		})
	})

	Describe("adaptive config mapping", func() {
		It("projects NATS config into pull-consumer settings", func() {
			cfg := config.NATSConfig{
				ResultAdaptiveEnabled:         true,
				ResultAdaptiveCheckInterval:   time.Second,
				ResultAdaptiveMediumPending:   10,
				ResultAdaptiveHighPending:     20,
				ResultAdaptiveLowBatchSize:    1,
				ResultAdaptiveLowMaxWait:      time.Millisecond,
				ResultAdaptiveMediumBatchSize: 2,
				ResultAdaptiveMediumMaxWait:   2 * time.Millisecond,
				ResultAdaptiveHighBatchSize:   3,
				ResultAdaptiveHighMaxWait:     3 * time.Millisecond,
			}

			adaptive := BuildAdaptiveConfig(cfg)

			Expect(adaptive.Enabled).To(BeTrue())
			Expect(adaptive.HighPending).To(Equal(20))
			Expect(adaptive.HighBatchSize).To(Equal(3))
		})
	})
})
