package scylla

import (
	"fmt"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/observability/cql"
)

// ConnectAndEnsureSchema opens the Scylla session and idempotently creates the
// keyspace and tables required by the saga.
func ConnectAndEnsureSchema(cfg config.ScyllaConfig, log logging.Logger) (*gocql.Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.QueryObserver = cql.Observer{Service: "registration-saga-service"}
	cluster.Port = cfg.Port
	cluster.Timeout = cfg.ConnectTimeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	if cfg.NumConns > 0 {
		cluster.NumConns = cfg.NumConns
	}
	cluster.MaxWaitSchemaAgreement = cfg.MaxWaitSchemaAgreement
	cluster.Consistency = parseConsistency(cfg.Consistency)
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	var sys *gocql.Session
	var err error
	for i := 0; i < cfg.RetryAttempts; i++ {
		sys, err = cluster.CreateSession()
		if err == nil {
			break
		}
		time.Sleep(cfg.RetryBackoff)
	}
	if err != nil {
		return nil, WrapCreateClusterSessionError(err)
	}
	defer sys.Close()

	if err := sys.Query(fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
	`, cfg.Keyspace)).Exec(); err != nil {
		return nil, WrapEnsureSchemaError(err)
	}

	cluster.Keyspace = cfg.Keyspace

	var app *gocql.Session
	for i := 0; i < cfg.RetryAttempts; i++ {
		app, err = cluster.CreateSession()
		if err == nil {
			break
		}
		time.Sleep(cfg.RetryBackoff)
	}
	if err != nil {
		return nil, WrapCreateClusterSessionError(err)
	}

	if err := app.Query(`
		CREATE TABLE IF NOT EXISTS registration_sessions (
			session_id TEXT PRIMARY KEY,
			client_id TEXT,
			user_id TEXT,
			email TEXT,
			username TEXT,
			status TEXT,
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		)
	`).Exec(); err != nil {
		app.Close()
		return nil, WrapEnsureSchemaError(err)
	}
	if err := ensureSessionColumns(app, cfg.Keyspace); err != nil {
		app.Close()
		return nil, WrapEnsureSchemaError(err)
	}

	if err := app.Query(`
		CREATE TABLE IF NOT EXISTS registration_sessions_by_email (
			email TEXT PRIMARY KEY,
			session_id TEXT,
			client_id TEXT,
			user_id TEXT,
			username TEXT,
			status TEXT,
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		)
	`).Exec(); err != nil {
		app.Close()
		return nil, WrapEnsureSchemaError(err)
	}

	if err := app.Query(`
		CREATE TABLE IF NOT EXISTS registration_sessions_by_username (
			username TEXT PRIMARY KEY,
			session_id TEXT,
			client_id TEXT,
			user_id TEXT,
			email TEXT,
			status TEXT,
			created_at TIMESTAMP,
			updated_at TIMESTAMP
		)
	`).Exec(); err != nil {
		app.Close()
		return nil, WrapEnsureSchemaError(err)
	}

	if err := app.Query(`
		CREATE TABLE IF NOT EXISTS registration_steps (
			session_id TEXT,
			step_key TEXT,
			status TEXT,
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			PRIMARY KEY ((session_id), step_key)
		)
	`).Exec(); err != nil {
		app.Close()
		return nil, WrapEnsureSchemaError(err)
	}

	if err := enableRegistrationCDC(app); err != nil {
		app.Close()
		return nil, WrapEnsureSchemaError(err)
	}
	if err := app.Query(`CREATE TABLE IF NOT EXISTS processed_events (
		event_id TEXT PRIMARY KEY, event_type TEXT, source_service TEXT,
		aggregate_type TEXT, aggregate_id TEXT, aggregate_version BIGINT,
		processed_at TIMESTAMP
	)`).Exec(); err != nil {
		app.Close()
		return nil, WrapEnsureSchemaError(err)
	}

	log.Info("scylla schema ensured", logging.String("keyspace", cfg.Keyspace))
	return app, nil
}

func enableRegistrationCDC(app *gocql.Session) error {
	for _, table := range []string{"registration_sessions", "registration_sessions_by_email", "registration_sessions_by_username", "registration_steps"} {
		if err := app.Query(fmt.Sprintf("ALTER TABLE %s WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true}", table)).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func parseConsistency(level string) gocql.Consistency {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "one":
		return gocql.One
	case "localquorum":
		return gocql.LocalQuorum
	case "all":
		return gocql.All
	default:
		return gocql.Quorum
	}
}

func ensureSessionColumns(app *gocql.Session, keyspace string) error {
	iter := app.Query(`
		SELECT column_name
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?
	`, keyspace, "registration_sessions").Iter()
	defer iter.Close()

	columns := map[string]struct{}{}
	var columnName string
	for iter.Scan(&columnName) {
		columns[columnName] = struct{}{}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if _, ok := columns["email"]; !ok {
		if err := app.Query(`ALTER TABLE registration_sessions ADD email TEXT`).Exec(); err != nil {
			return err
		}
	}
	if _, ok := columns["username"]; !ok {
		if err := app.Query(`ALTER TABLE registration_sessions ADD username TEXT`).Exec(); err != nil {
			return err
		}
	}

	return nil
}
