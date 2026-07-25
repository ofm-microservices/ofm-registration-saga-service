package scylla

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"registration-saga-service/config"
)

var _ = Describe("ConnectAndEnsureSchema", func() {
	BeforeEach(func() {
		if os.Getenv("RUN_SCYLLA_INTEGRATION") != "1" {
			Skip("scylla integration tests are opt-in; set RUN_SCYLLA_INTEGRATION=1 to run them")
		}
	})

	It("creates the keyspace and required tables idempotently", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()

		container, cfg := startStorageScyllaContainer(ctx, "registration_saga_store")
		defer func() {
			Expect(container.Terminate(context.Background())).To(Succeed())
		}()

		logger, err := logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())

		session, err := ConnectAndEnsureSchema(cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		defer session.Close()

		// Idempotent second pass covers the bootstrap update path too.
		sessionAgain, err := ConnectAndEnsureSchema(cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		sessionAgain.Close()

		var tableName string
		iter := session.Query(`
			SELECT table_name
			FROM system_schema.tables
			WHERE keyspace_name = ?
		`, cfg.Keyspace).Iter()

		tables := map[string]struct{}{}
		for iter.Scan(&tableName) {
			tables[tableName] = struct{}{}
		}
		Expect(iter.Close()).To(Succeed())

		Expect(tables).To(HaveKey("registration_sessions"))
		Expect(tables).To(HaveKey("registration_sessions_by_email"))
		Expect(tables).To(HaveKey("registration_sessions_by_username"))
		Expect(tables).To(HaveKey("registration_steps"))
	})

	It("wraps cluster connection failures", func() {
		logger, err := logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())

		_, err = ConnectAndEnsureSchema(config.ScyllaConfig{
			Hosts:                  []string{"127.0.0.1"},
			Port:                   1,
			Keyspace:               "registration_saga_store",
			Username:               "test-user",
			Password:               "test-password",
			Consistency:            "quorum",
			ConnectTimeout:         50 * time.Millisecond,
			MaxWaitSchemaAgreement: 50 * time.Millisecond,
			RetryAttempts:          1,
			RetryBackoff:           10 * time.Millisecond,
		}, logger)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("create scylla session"))
	})

	It("wraps schema creation failures for invalid keyspace names", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()

		container, cfg := startStorageScyllaContainer(ctx, "invalid-keyspace")
		defer func() {
			Expect(container.Terminate(context.Background())).To(Succeed())
		}()

		logger, err := logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())

		session, err := ConnectAndEnsureSchema(cfg, logger)

		Expect(session).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ensure scylla schema"))
	})

	It("backfills missing registration session columns", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()

		container, cfg := startStorageScyllaContainer(ctx, "registration_saga_legacy")
		defer func() {
			Expect(container.Terminate(context.Background())).To(Succeed())
		}()

		logger, err := logging.New("registration-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())

		cluster := gocql.NewCluster(cfg.Hosts...)
		cluster.Port = cfg.Port
		cluster.Timeout = cfg.ConnectTimeout
		cluster.ConnectTimeout = cfg.ConnectTimeout
		cluster.MaxWaitSchemaAgreement = cfg.MaxWaitSchemaAgreement

		sys, err := cluster.CreateSession()
		Expect(err).NotTo(HaveOccurred())
		Expect(sys.Query(`
			CREATE KEYSPACE IF NOT EXISTS registration_saga_legacy
			WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
		`).Exec()).To(Succeed())
		sys.Close()

		cluster.Keyspace = cfg.Keyspace
		app, err := cluster.CreateSession()
		Expect(err).NotTo(HaveOccurred())
		Expect(app.Query(`
			CREATE TABLE registration_sessions (
				session_id TEXT PRIMARY KEY,
				client_id TEXT,
				user_id TEXT,
				status TEXT,
				created_at TIMESTAMP,
				updated_at TIMESTAMP
			)
		`).Exec()).To(Succeed())

		Expect(ensureSessionColumns(app, cfg.Keyspace)).To(Succeed())

		var columnName string
		iter := app.Query(`
			SELECT column_name
			FROM system_schema.columns
			WHERE keyspace_name = ? AND table_name = ?
		`, cfg.Keyspace, "registration_sessions").Iter()

		columns := map[string]struct{}{}
		for iter.Scan(&columnName) {
			columns[columnName] = struct{}{}
		}
		Expect(iter.Close()).To(Succeed())
		app.Close()

		Expect(columns).To(HaveKey("email"))
		Expect(columns).To(HaveKey("username"))

		session, err := ConnectAndEnsureSchema(cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		session.Close()
	})
})

var _ = Describe("parseConsistency", func() {
	It("maps supported consistency names and defaults to quorum", func() {
		Expect(parseConsistency("one")).To(Equal(gocql.One))
		Expect(parseConsistency("localquorum")).To(Equal(gocql.LocalQuorum))
		Expect(parseConsistency("all")).To(Equal(gocql.All))
		Expect(parseConsistency(" something-else ")).To(Equal(gocql.Quorum))
	})
})

func startStorageScyllaContainer(ctx context.Context, keyspace string) (testcontainers.Container, config.ScyllaConfig) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "scylladb/scylla:6.1",
			ExposedPorts: []string{"9042/tcp"},
			Cmd:          []string{"--smp", "1", "--memory", "512M", "--overprovisioned", "1"},
			WaitingFor:   wait.ForListeningPort("9042/tcp").WithStartupTimeout(6 * time.Minute),
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
