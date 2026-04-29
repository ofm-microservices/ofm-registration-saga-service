package config

import "fmt"

// WrapParseEnvConfigError annotates environment parsing failures with config
// loading context.
func WrapParseEnvConfigError(err error) error {
	return fmt.Errorf("parse env config: %w", err)
}
