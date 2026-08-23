package config

import "time"

// KafkaConfig defines the Kafka broker and consumer group used by the registration saga.
type KafkaConfig struct {
	URL                            string        `env:"KAFKA_LEGACY_NATS_URL"`
	User                           string        `env:"KAFKA_LEGACY_NATS_USER"`
	Password                       string        `env:"KAFKA_LEGACY_NATS_PASSWORD"`
	Brokers                        []string      `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	GroupID                        string        `env:"KAFKA_REGISTRATION_GROUP_ID" envDefault:"registration-saga-service"`
	UserResultTopic                string        `env:"KAFKA_REGISTRATION_USER_RESULT_TOPIC" envDefault:"saga.user.create.result"`
	AuthResultTopic                string        `env:"KAFKA_REGISTRATION_AUTH_RESULT_TOPIC" envDefault:"saga.auth.create_pending_registration.result"`
	MailResultTopic                string        `env:"KAFKA_REGISTRATION_MAIL_RESULT_TOPIC" envDefault:"mail.send.result"`
	RegistrationEventsStream       string        `env:"KAFKA_REGISTRATION_EVENTS_STREAM" envDefault:"REGISTRATION_EVENTS"`
	UserEventsStream               string        `env:"KAFKA_USER_EVENTS_STREAM" envDefault:"USER_EVENTS"`
	AuthEventsStream               string        `env:"KAFKA_AUTH_EVENTS_STREAM" envDefault:"AUTH_EVENTS"`
	MailEventsStream               string        `env:"KAFKA_MAIL_EVENTS_STREAM" envDefault:"MAIL_EVENTS"`
	RegistrationCodeSentSubject    string        `env:"KAFKA_REGISTRATION_CODE_SENT_TOPIC" envDefault:"registration.code.sent"`
	RegistrationCompletedSubject   string        `env:"KAFKA_REGISTRATION_COMPLETED_TOPIC" envDefault:"registration.completed"`
	RegistrationFailedSubject      string        `env:"KAFKA_REGISTRATION_FAILED_TOPIC" envDefault:"registration.failed"`
	UserCreateSubject              string        `env:"KAFKA_SAGA_USER_CREATE_TOPIC" envDefault:"saga.user.create"`
	UserCreateResultSubject        string        `env:"KAFKA_REGISTRATION_USER_RESULT_TOPIC" envDefault:"saga.user.create.result"`
	AuthCreatePendingSubject       string        `env:"KAFKA_SAGA_AUTH_CREATE_TOPIC" envDefault:"saga.auth.create_pending_registration"`
	AuthCreatePendingResultSubject string        `env:"KAFKA_REGISTRATION_AUTH_RESULT_TOPIC" envDefault:"saga.auth.create_pending_registration.result"`
	MailSendResultSubject          string        `env:"KAFKA_REGISTRATION_MAIL_RESULT_TOPIC" envDefault:"mail.send.result"`
	UserCreateResultDurable        string        `env:"KAFKA_USER_RESULT_DURABLE" envDefault:"registration_saga_user_create_result"`
	AuthCreatePendingResultDurable string        `env:"KAFKA_AUTH_RESULT_DURABLE" envDefault:"registration_saga_auth_create_pending_result"`
	MailSendResultDurable          string        `env:"KAFKA_MAIL_RESULT_DURABLE" envDefault:"registration_saga_mail_send_result"`
	ResultBatchSize                int           `env:"KAFKA_RESULT_BATCH_SIZE" envDefault:"32"`
	ResultMaxWait                  time.Duration `env:"KAFKA_RESULT_MAX_WAIT" envDefault:"10ms"`
	ResultWorkers                  int           `env:"KAFKA_RESULT_WORKERS" envDefault:"8"`
	ResultQueueSize                int           `env:"KAFKA_RESULT_QUEUE_SIZE" envDefault:"500"`
	ResultAckWait                  time.Duration `env:"KAFKA_RESULT_ACK_WAIT" envDefault:"30s"`
	ResultMaxDeliver               int           `env:"KAFKA_RESULT_MAX_DELIVER" envDefault:"5"`
	ResultAdaptiveEnabled          bool          `env:"KAFKA_RESULT_ADAPTIVE_ENABLED" envDefault:"false"`
	ResultAdaptiveCheckInterval    time.Duration `env:"KAFKA_RESULT_ADAPTIVE_CHECK_INTERVAL" envDefault:"2s"`
	ResultAdaptiveMediumPending    int           `env:"KAFKA_RESULT_ADAPTIVE_MEDIUM_PENDING" envDefault:"200"`
	ResultAdaptiveHighPending      int           `env:"KAFKA_RESULT_ADAPTIVE_HIGH_PENDING" envDefault:"1000"`
	ResultAdaptiveLowBatchSize     int           `env:"KAFKA_RESULT_ADAPTIVE_LOW_BATCH_SIZE" envDefault:"8"`
	ResultAdaptiveLowMaxWait       time.Duration `env:"KAFKA_RESULT_ADAPTIVE_LOW_MAX_WAIT" envDefault:"25ms"`
	ResultAdaptiveMediumBatchSize  int           `env:"KAFKA_RESULT_ADAPTIVE_MEDIUM_BATCH_SIZE" envDefault:"32"`
	ResultAdaptiveMediumMaxWait    time.Duration `env:"KAFKA_RESULT_ADAPTIVE_MEDIUM_MAX_WAIT" envDefault:"10ms"`
	ResultAdaptiveHighBatchSize    int           `env:"KAFKA_RESULT_ADAPTIVE_HIGH_BATCH_SIZE" envDefault:"128"`
	ResultAdaptiveHighMaxWait      time.Duration `env:"KAFKA_RESULT_ADAPTIVE_HIGH_MAX_WAIT" envDefault:"2ms"`
}
