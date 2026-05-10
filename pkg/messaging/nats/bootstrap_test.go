package nats

import (
	"errors"
	"testing"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"registration-saga-service/config"
)

func TestBootstrap(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Messaging NATS Suite")
}

var _ = Describe("NATS bootstrap", func() {
	var logger logging.Logger

	BeforeEach(func() {
		var err error
		logger, err = logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	It("validates nil logger", func() {
		Expect(EnsureStream(config.NATSConfig{}, nil)).To(MatchError(ErrNilLogger))
	})

	It("wraps connection failures", func() {
		_, err := Connect(config.NATSConfig{URL: "bad://url"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("connect to nats"))

		err = EnsureStream(config.NATSConfig{
			URL:                            "nats://127.0.0.1:1",
			RegistrationEventsStream:       "REGISTRATION_EVENTS",
			RegistrationCodeSentSubject:    "registration.code.sent",
			UserEventsStream:               "USER_EVENTS",
			UserCreateResultSubject:        "saga.user.create.result",
			AuthEventsStream:               "AUTH_EVENTS",
			AuthCreatePendingResultSubject: "saga.auth.create_pending.result",
			MailEventsStream:               "MAIL_EVENTS",
			MailSendResultSubject:          "mail.send.result",
		}, logger)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("connect to nats"))
	})

	It("wraps helper errors", func() {
		err := errors.New("boom")
		Expect(WrapInitJetStreamContextError(err)).To(MatchError(ContainSubstring("init jetstream context")))
		Expect(WrapEnsureStreamError("stream", err, err)).To(MatchError(ContainSubstring(`ensure stream "stream"`)))
	})
})
