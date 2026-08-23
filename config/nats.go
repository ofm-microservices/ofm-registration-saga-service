package config

import "time"

// NATSConfig is retained solely for compatibility with legacy adapters and
// configuration tests; production registration wiring uses KafkaConfig.
type NATSConfig struct {
	URL                            string        `env:"NATS_URL"`
	User                           string        `env:"NATS_USER"`
	Password                       string        `env:"NATS_PASSWORD"`
	RegistrationEventsStream       string        `env:"NATS_STREAM_REGISTRATION_EVENTS" envDefault:"REGISTRATION_EVENTS"`
	UserEventsStream               string        `env:"NATS_STREAM_USER_EVENTS" envDefault:"USER_EVENTS"`
	AuthEventsStream               string        `env:"NATS_STREAM_AUTH_EVENTS" envDefault:"AUTH_EVENTS"`
	MailEventsStream               string        `env:"NATS_STREAM_MAIL_EVENTS" envDefault:"MAIL_EVENTS"`
	RegistrationCodeSentSubject    string        `env:"NATS_SUBJECT_REGISTRATION_CODE_SENT" envDefault:"registration.code.sent"`
	RegistrationCompletedSubject   string        `env:"NATS_SUBJECT_REGISTRATION_COMPLETED" envDefault:"registration.completed"`
	RegistrationFailedSubject      string        `env:"NATS_SUBJECT_REGISTRATION_FAILED" envDefault:"registration.failed"`
	UserCreateSubject              string        `env:"NATS_SUBJECT_SAGA_CREATE_USER" envDefault:"saga.user.create"`
	UserCreateResultSubject        string        `env:"NATS_SUBJECT_SAGA_CREATE_USER_RESULT" envDefault:"saga.user.create.result"`
	AuthCreatePendingSubject       string        `env:"NATS_SUBJECT_SAGA_CREATE_PENDING_AUTH" envDefault:"saga.auth.create_pending_registration"`
	AuthCreatePendingResultSubject string        `env:"NATS_SUBJECT_SAGA_CREATE_PENDING_AUTH_RESULT" envDefault:"saga.auth.create_pending_registration.result"`
	MailSendResultSubject          string        `env:"NATS_SUBJECT_MAIL_SEND_RESULT" envDefault:"mail.send.result"`
	UserCreateResultDurable        string        `env:"NATS_DURABLE_SAGA_CREATE_USER_RESULT" envDefault:"registration_saga_user_create_result"`
	AuthCreatePendingResultDurable string        `env:"NATS_DURABLE_SAGA_CREATE_PENDING_AUTH_RESULT" envDefault:"registration_saga_auth_create_pending_result"`
	MailSendResultDurable          string        `env:"NATS_DURABLE_MAIL_SEND_RESULT" envDefault:"registration_saga_mail_send_result"`
	ResultBatchSize                int           `env:"NATS_RESULT_BATCH_SIZE" envDefault:"32"`
	ResultMaxWait                  time.Duration `env:"NATS_RESULT_MAX_WAIT" envDefault:"10ms"`
	ResultWorkers                  int           `env:"NATS_RESULT_WORKERS" envDefault:"8"`
	ResultQueueSize                int           `env:"NATS_RESULT_QUEUE_SIZE" envDefault:"500"`
	ResultAckWait                  time.Duration `env:"NATS_RESULT_ACK_WAIT" envDefault:"30s"`
	ResultMaxDeliver               int           `env:"NATS_RESULT_MAX_DELIVER" envDefault:"5"`
	ResultAdaptiveEnabled          bool          `env:"NATS_RESULT_ADAPTIVE_ENABLED" envDefault:"false"`
	ResultAdaptiveCheckInterval    time.Duration `env:"NATS_RESULT_ADAPTIVE_CHECK_INTERVAL" envDefault:"2s"`
	ResultAdaptiveMediumPending    int           `env:"NATS_RESULT_ADAPTIVE_MEDIUM_PENDING" envDefault:"200"`
	ResultAdaptiveHighPending      int           `env:"NATS_RESULT_ADAPTIVE_HIGH_PENDING" envDefault:"1000"`
	ResultAdaptiveLowBatchSize     int           `env:"NATS_RESULT_ADAPTIVE_LOW_BATCH_SIZE" envDefault:"8"`
	ResultAdaptiveLowMaxWait       time.Duration `env:"NATS_RESULT_ADAPTIVE_LOW_MAX_WAIT" envDefault:"25ms"`
	ResultAdaptiveMediumBatchSize  int           `env:"NATS_RESULT_ADAPTIVE_MEDIUM_BATCH_SIZE" envDefault:"32"`
	ResultAdaptiveMediumMaxWait    time.Duration `env:"NATS_RESULT_ADAPTIVE_MEDIUM_MAX_WAIT" envDefault:"10ms"`
	ResultAdaptiveHighBatchSize    int           `env:"NATS_RESULT_ADAPTIVE_HIGH_BATCH_SIZE" envDefault:"128"`
	ResultAdaptiveHighMaxWait      time.Duration `env:"NATS_RESULT_ADAPTIVE_HIGH_MAX_WAIT" envDefault:"2ms"`
}
