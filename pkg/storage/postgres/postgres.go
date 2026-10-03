package postgres

import (
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/attribute"
	"net/url"
	"path/filepath"
	"registration-saga-service/config"
	"strings"
	"time"
)

// Open opens the registration saga PostgreSQL pool.
func Open(c config.DBConfig) (*sqlx.DB, error) {
	d, e := otelsql.Register("pgx", otelsql.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.namespace", c.Name)))
	if e != nil {
		return nil, e
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", url.QueryEscape(c.User), url.QueryEscape(c.Password), c.Host, c.Port, url.QueryEscape(c.Name), url.QueryEscape(c.SSLMode))
	db, e := sqlx.Connect(d, dsn)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(c.MaxOpenConns)
	db.SetMaxIdleConns(c.MaxIdleConns)
	db.SetConnMaxLifetime(c.ConnMaxLifetime)
	db.SetConnMaxIdleTime(2 * time.Minute)
	return db, nil
}

// RunMigrations applies registration saga PostgreSQL migrations.
func RunMigrations(c config.DBConfig) error {
	p := c.MigrationsPath
	if x, ok := strings.CutPrefix(p, "file://"); ok && !filepath.IsAbs(x) {
		a, e := filepath.Abs(x)
		if e != nil {
			return e
		}
		p = "file://" + a
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&x-migrations-table=%s", url.QueryEscape(c.User), url.QueryEscape(c.Password), c.Host, c.Port, url.QueryEscape(c.Name), url.QueryEscape(c.SSLMode), url.QueryEscape(c.MigrationsTable))
	m, e := migrate.New(p, dsn)
	if e != nil {
		return e
	}
	defer m.Close()
	e = m.Up()
	if e != nil && e != migrate.ErrNoChange {
		return e
	}
	return nil
}
