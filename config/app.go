package config

// AppConfig groups process-level runtime settings for the service itself.
type AppConfig struct {
	Env      string `env:"APP_ENV" envDefault:"local"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}
