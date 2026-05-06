package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"registration-saga-service/config"
)

var (
	bootstrapJSContainer   testcontainers.Container
	bootstrapNoJSContainer testcontainers.Container
	bootstrapJSCfg         config.NATSConfig
	bootstrapNoJSCfg       config.NATSConfig
	bootstrapLogger        logging.Logger
)

var _ = BeforeSuite(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var err error
	bootstrapLogger, err = logging.New("registration-saga-service", "test", "debug")
	Expect(err).NotTo(HaveOccurred())

	bootstrapJSContainer, bootstrapJSCfg = startBootstrapNATSContainer(ctx, true)
	bootstrapNoJSContainer, bootstrapNoJSCfg = startBootstrapNATSContainer(ctx, false)
})

var _ = AfterSuite(func() {
	if bootstrapJSContainer != nil {
		Expect(bootstrapJSContainer.Terminate(context.Background())).To(Succeed())
	}
	if bootstrapNoJSContainer != nil {
		Expect(bootstrapNoJSContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("bootstrap integration", func() {
	It("connects to a real nats server", func() {
		nc, err := Connect(bootstrapJSCfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(nc.IsConnected()).To(BeTrue())
		nc.Close()
	})

	It("connects when optional credentials are present in config", func() {
		cfg := bootstrapJSCfg
		cfg.User = "ignored-user"
		cfg.Password = "ignored-pass"

		nc, err := Connect(cfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(nc.IsConnected()).To(BeTrue())
		nc.Close()
	})

	It("creates and then updates the required streams", func() {
		Expect(EnsureStream(bootstrapJSCfg, bootstrapLogger)).To(Succeed())
		Expect(EnsureStream(bootstrapJSCfg, bootstrapLogger)).To(Succeed())

		nc, err := nats.Connect(bootstrapJSCfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer nc.Close()

		js, err := nc.JetStream()
		Expect(err).NotTo(HaveOccurred())

		registrationInfo, err := js.StreamInfo(bootstrapJSCfg.RegistrationEventsStream)
		Expect(err).NotTo(HaveOccurred())
		Expect(registrationInfo.Config.Subjects).To(ContainElement(bootstrapJSCfg.RegistrationCodeSentSubject))

		userInfo, err := js.StreamInfo(bootstrapJSCfg.UserEventsStream)
		Expect(err).NotTo(HaveOccurred())
		Expect(userInfo.Config.Subjects).To(ContainElement(bootstrapJSCfg.UserCreateResultSubject))

		authInfo, err := js.StreamInfo(bootstrapJSCfg.AuthEventsStream)
		Expect(err).NotTo(HaveOccurred())
		Expect(authInfo.Config.Subjects).To(ContainElement(bootstrapJSCfg.AuthCreatePendingResultSubject))

		mailInfo, err := js.StreamInfo(bootstrapJSCfg.MailEventsStream)
		Expect(err).NotTo(HaveOccurred())
		Expect(mailInfo.Config.Subjects).To(ContainElement(bootstrapJSCfg.MailSendResultSubject))
	})

	It("requires a logger", func() {
		Expect(EnsureStream(bootstrapJSCfg, nil)).To(MatchError(ErrNilLogger))
	})

	It("wraps connect failures", func() {
		cfg := bootstrapJSCfg
		cfg.URL = "nats://127.0.0.1:1"

		_, err := Connect(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("connect to nats"))
	})

	It("wraps jetstream initialization failures when the server has no jetstream", func() {
		err := EnsureStream(bootstrapNoJSCfg, bootstrapLogger)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`ensure stream "` + bootstrapNoJSCfg.RegistrationEventsStream + `"`))
	})
})

var _ = Describe("error wrappers", func() {
	It("preserves wrapped causes", func() {
		cause := errors.New("boom")

		Expect(WrapInitJetStreamContextError(cause)).To(MatchError(ContainSubstring("init jetstream context")))
		Expect(WrapEnsureStreamError("STREAM", cause, cause)).To(MatchError(ContainSubstring(`ensure stream "STREAM"`)))
		Expect(WrapConnectToNATSError(cause)).To(MatchError(ContainSubstring("connect to nats")))
	})
})

func startBootstrapNATSContainer(ctx context.Context, jetstream bool) (testcontainers.Container, config.NATSConfig) {
	cmd := []string{}
	if jetstream {
		cmd = []string{"-js"}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.12.4-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          cmd,
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
		UserCreateResultSubject:        "saga.user.create.result",
		AuthCreatePendingResultSubject: "saga.auth.create_pending_registration.result",
		MailSendResultSubject:          "mail.send.result",
	}
}
