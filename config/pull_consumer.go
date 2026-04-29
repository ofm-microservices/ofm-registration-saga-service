package config

import "time"

// PullAdaptiveConfig describes optional runtime tuning thresholds for a pull
// consumer that changes batch behaviour based on backlog size.
type PullAdaptiveConfig struct {
	Enabled         bool
	CheckInterval   time.Duration
	MediumPending   int
	HighPending     int
	LowBatchSize    int
	LowMaxWait      time.Duration
	MediumBatchSize int
	MediumMaxWait   time.Duration
	HighBatchSize   int
	HighMaxWait     time.Duration
}

// PullConsumerConfig is the normalized runtime view of one JetStream pull
// consumer.
type PullConsumerConfig struct {
	Stream     string
	Subject    string
	Durable    string
	BatchSize  int
	MaxWait    time.Duration
	Workers    int
	QueueSize  int
	AckWait    time.Duration
	MaxDeliver int
	Adaptive   PullAdaptiveConfig
}
