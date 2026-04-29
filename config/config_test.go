package config

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestConfig(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Suite")
}

var _ = Describe("Load", func() {
	setEnv := func(key, value string) {
		prev, hadPrev := os.LookupEnv(key)
		Expect(os.Setenv(key, value)).To(Succeed())
		DeferCleanup(func() {
			if hadPrev {
				_ = os.Setenv(key, prev)
				return
			}
			_ = os.Unsetenv(key)
		})
	}

	It("loads explicit environment values", func() {
		setEnv("APP_ENV", "test")
		setEnv("LOG_LEVEL", "debug")
		setEnv("GRPC_HOST", "127.0.0.1")
		setEnv("GRPC_PORT", "9095")
		setEnv("NATS_URL", "nats://localhost:4222")
		setEnv("SCYLLA_HOSTS", "db-1,db-2")
		setEnv("AUTH_SERVICE_ADDRESS", "127.0.0.1:9091")
		setEnv("USER_SERVICE_ADDRESS", "127.0.0.1:9092")

		cfg, err := Load()

		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.App.Env).To(Equal("test"))
		Expect(cfg.App.LogLevel).To(Equal("debug"))
		Expect(cfg.GRPC.Host).To(Equal("127.0.0.1"))
		Expect(cfg.GRPC.Port).To(Equal(9095))
		Expect(cfg.NATS.URL).To(Equal("nats://localhost:4222"))
		Expect(cfg.Scylla.Hosts).To(Equal([]string{"db-1", "db-2"}))
		Expect(cfg.AuthService.Address).To(Equal("127.0.0.1:9091"))
		Expect(cfg.UserService.Address).To(Equal("127.0.0.1:9092"))
	})

	It("wraps parse failures", func() {
		setEnv("NATS_URL", "nats://localhost:4222")
		setEnv("GRPC_PORT", "bad-port")

		_, err := Load()

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("parse env config"))
	})
})
