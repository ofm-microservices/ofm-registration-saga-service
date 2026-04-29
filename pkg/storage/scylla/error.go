package scylla

import "fmt"

// WrapCreateClusterSessionError annotates Scylla session creation failures.
func WrapCreateClusterSessionError(err error) error {
	return fmt.Errorf("create scylla session: %w", err)
}

// WrapEnsureSchemaError annotates keyspace or table bootstrap failures.
func WrapEnsureSchemaError(err error) error {
	return fmt.Errorf("ensure scylla schema: %w", err)
}
