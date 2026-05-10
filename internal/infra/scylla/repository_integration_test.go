package scylla

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"registration-saga-service/config"
	"registration-saga-service/internal/domain"
	pkgscylla "registration-saga-service/pkg/storage/scylla"
)

var (
	repoSuiteContainer testcontainers.Container
	repoSuiteCfg       config.ScyllaConfig
	repoSuiteSession   *gocql.Session
)

var _ = BeforeSuite(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	repoSuiteContainer, repoSuiteCfg = startScyllaContainer(ctx, "registration_saga_repo")

	var err error
	repoSuiteSession, err = pkgscylla.ConnectAndEnsureSchema(repoSuiteCfg, suiteLogger())
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if repoSuiteSession != nil {
		repoSuiteSession.Close()
	}
	if repoSuiteContainer != nil {
		Expect(repoSuiteContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("Scylla repositories integration", func() {
	var (
		sessionRepo domain.SessionRepository
		stepRepo    domain.StepRepository
	)

	BeforeEach(func() {
		truncateScyllaTables(repoSuiteSession)

		var err error
		sessionRepo, err = NewSessionRepository(repoSuiteSession)
		Expect(err).NotTo(HaveOccurred())

		stepRepo, err = NewStepRepository(repoSuiteSession)
		Expect(err).NotTo(HaveOccurred())
	})

	It("creates and loads sessions through all access paths", func() {
		created, err := sessionRepo.Create(context.Background(), domain.Session{
			SessionID: "session-1",
			ClientID:  "client-1",
			UserID:    "user-1",
			Email:     "alex@example.com",
			Username:  "alex",
			Status:    domain.SessionStatusStarted,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created.CreatedAt.IsZero()).To(BeFalse())
		Expect(created.UpdatedAt.IsZero()).To(BeFalse())

		byID, err := sessionRepo.GetByID(context.Background(), "session-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(byID.Email).To(Equal("alex@example.com"))

		byEmail, err := sessionRepo.GetByEmail(context.Background(), "alex@example.com")
		Expect(err).NotTo(HaveOccurred())
		Expect(byEmail.Username).To(Equal("alex"))

		byUsername, err := sessionRepo.GetByUsername(context.Background(), "alex")
		Expect(err).NotTo(HaveOccurred())
		Expect(byUsername.UserID).To(Equal("user-1"))
	})

	It("maps missing session lookups to domain not found errors", func() {
		found, err := sessionRepo.GetByID(context.Background(), "missing")
		Expect(found).To(BeNil())
		Expect(err).To(MatchError(domain.ErrSessionNotFound))

		found, err = sessionRepo.GetByEmail(context.Background(), "missing@example.com")
		Expect(found).To(BeNil())
		Expect(err).To(MatchError(domain.ErrSessionNotFound))

		found, err = sessionRepo.GetByUsername(context.Background(), "missing")
		Expect(found).To(BeNil())
		Expect(err).To(MatchError(domain.ErrSessionNotFound))
	})

	It("returns not found when updating a missing session", func() {
		err := sessionRepo.UpdateStatus(context.Background(), "missing", domain.SessionStatusFailed)

		Expect(err).To(MatchError(domain.ErrSessionNotFound))
	})

	It("updates session status in the primary and denormalized tables", func() {
		_, err := sessionRepo.Create(context.Background(), domain.Session{
			SessionID: "session-2",
			ClientID:  "client-2",
			UserID:    "user-2",
			Email:     "sam@example.com",
			Username:  "sam",
			Status:    domain.SessionStatusStarted,
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(sessionRepo.UpdateStatus(context.Background(), "session-2", domain.SessionStatusFailed)).To(Succeed())

		byID, err := sessionRepo.GetByID(context.Background(), "session-2")
		Expect(err).NotTo(HaveOccurred())
		Expect(byID.Status).To(Equal(domain.SessionStatusFailed))

		byEmail, err := sessionRepo.GetByEmail(context.Background(), "sam@example.com")
		Expect(err).NotTo(HaveOccurred())
		Expect(byEmail.Status).To(Equal(domain.SessionStatusFailed))

		byUsername, err := sessionRepo.GetByUsername(context.Background(), "sam")
		Expect(err).NotTo(HaveOccurred())
		Expect(byUsername.Status).To(Equal(domain.SessionStatusFailed))
	})

	It("wraps create failures when a denormalized session table is unavailable", func() {
		tempSession := openTempScyllaSession("repo_create_fail")
		defer tempSession.Close()

		tempRepo, err := NewSessionRepository(tempSession)
		Expect(err).NotTo(HaveOccurred())

		Expect(tempSession.Query("DROP TABLE registration_sessions_by_email").Exec()).To(Succeed())

		created, err := tempRepo.Create(context.Background(), domain.Session{
			SessionID: "session-create-failure",
			ClientID:  "client-create-failure",
			UserID:    "user-create-failure",
			Email:     "create.failure@example.com",
			Username:  "create-failure",
			Status:    domain.SessionStatusStarted,
		})

		Expect(created).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("create registration session"))
	})

	It("wraps create failures when the username session table is unavailable", func() {
		tempSession := openTempScyllaSession("repo_create_user")
		defer tempSession.Close()

		tempRepo, err := NewSessionRepository(tempSession)
		Expect(err).NotTo(HaveOccurred())

		Expect(tempSession.Query("DROP TABLE registration_sessions_by_username").Exec()).To(Succeed())

		created, err := tempRepo.Create(context.Background(), domain.Session{
			SessionID: "session-create-username-failure",
			ClientID:  "client-create-username-failure",
			UserID:    "user-create-username-failure",
			Email:     "create.username.failure@example.com",
			Username:  "create-username-failure",
			Status:    domain.SessionStatusStarted,
		})

		Expect(created).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("create registration session"))
	})

	It("wraps update failures when a denormalized session table is unavailable", func() {
		tempSession := openTempScyllaSession("repo_update_fail")
		defer tempSession.Close()

		tempRepo, err := NewSessionRepository(tempSession)
		Expect(err).NotTo(HaveOccurred())

		created, err := tempRepo.Create(context.Background(), domain.Session{
			SessionID: "session-update-failure",
			ClientID:  "client-update-failure",
			UserID:    "user-update-failure",
			Email:     "update.failure@example.com",
			Username:  "update-failure",
			Status:    domain.SessionStatusStarted,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).NotTo(BeNil())

		Expect(tempSession.Query("DROP TABLE registration_sessions_by_email").Exec()).To(Succeed())

		err = tempRepo.UpdateStatus(context.Background(), "session-update-failure", domain.SessionStatusFailed)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("update registration session status"))
	})

	It("wraps update failures when the username session table is unavailable", func() {
		tempSession := openTempScyllaSession("repo_update_user")
		defer tempSession.Close()

		tempRepo, err := NewSessionRepository(tempSession)
		Expect(err).NotTo(HaveOccurred())

		created, err := tempRepo.Create(context.Background(), domain.Session{
			SessionID: "session-update-username-failure",
			ClientID:  "client-update-username-failure",
			UserID:    "user-update-username-failure",
			Email:     "update.username.failure@example.com",
			Username:  "update-username-failure",
			Status:    domain.SessionStatusStarted,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).NotTo(BeNil())

		Expect(tempSession.Query("DROP TABLE registration_sessions_by_username").Exec()).To(Succeed())

		err = tempRepo.UpdateStatus(context.Background(), "session-update-username-failure", domain.SessionStatusFailed)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("update registration session status"))
	})

	It("creates, loads, lists, and updates steps", func() {
		created, err := stepRepo.Create(context.Background(), domain.Step{
			SessionID: "session-3",
			StepKey:   domain.StepKeyUserCreateProfile,
			Status:    domain.StepStatusPending,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created.CreatedAt.IsZero()).To(BeFalse())

		_, err = stepRepo.Create(context.Background(), domain.Step{
			SessionID: "session-3",
			StepKey:   domain.StepKeyAuthCreatePending,
			Status:    domain.StepStatusInProgress,
		})
		Expect(err).NotTo(HaveOccurred())

		loaded, err := stepRepo.GetByKey(context.Background(), "session-3", domain.StepKeyUserCreateProfile)
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.Status).To(Equal(domain.StepStatusPending))

		listed, err := stepRepo.ListBySessionID(context.Background(), "session-3")
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(HaveLen(2))

		Expect(stepRepo.UpdateStatus(context.Background(), "session-3", domain.StepKeyUserCreateProfile, domain.StepStatusCompleted)).To(Succeed())

		loaded, err = stepRepo.GetByKey(context.Background(), "session-3", domain.StepKeyUserCreateProfile)
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.Status).To(Equal(domain.StepStatusCompleted))
	})

	It("maps missing steps to domain not found", func() {
		step, err := stepRepo.GetByKey(context.Background(), "missing", domain.StepKeyUserCreateProfile)
		Expect(step).To(BeNil())
		Expect(err).To(MatchError(domain.ErrStepNotFound))

		listed, err := stepRepo.ListBySessionID(context.Background(), "missing")
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(BeEmpty())
	})

	It("wraps repository errors when using a closed session", func() {
		session, err := pkgscylla.ConnectAndEnsureSchema(repoSuiteCfg, suiteLogger())
		Expect(err).NotTo(HaveOccurred())

		closedSessionRepo, err := NewSessionRepository(session)
		Expect(err).NotTo(HaveOccurred())
		closedStepRepo, err := NewStepRepository(session)
		Expect(err).NotTo(HaveOccurred())

		session.Close()

		created, err := closedSessionRepo.Create(context.Background(), domain.Session{
			SessionID: "session-closed",
			ClientID:  "client-closed",
			UserID:    "user-closed",
			Email:     "closed@example.com",
			Username:  "closed",
			Status:    domain.SessionStatusStarted,
		})
		Expect(created).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("create registration session"))

		found, err := closedSessionRepo.GetByID(context.Background(), "session-closed")
		Expect(found).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("get registration session by id"))

		found, err = closedSessionRepo.GetByEmail(context.Background(), "closed@example.com")
		Expect(found).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("get registration session by email"))

		found, err = closedSessionRepo.GetByUsername(context.Background(), "closed")
		Expect(found).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("get registration session by username"))

		Expect(closedSessionRepo.UpdateStatus(context.Background(), "session-closed", domain.SessionStatusFailed)).To(HaveOccurred())

		step, err := closedStepRepo.Create(context.Background(), domain.Step{
			SessionID: "session-closed",
			StepKey:   domain.StepKeyUserCreateProfile,
			Status:    domain.StepStatusPending,
		})
		Expect(step).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("create registration step"))

		foundStep, err := closedStepRepo.GetByKey(context.Background(), "session-closed", domain.StepKeyUserCreateProfile)
		Expect(foundStep).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("get registration step by key"))

		listed, err := closedStepRepo.ListBySessionID(context.Background(), "session-closed")
		Expect(listed).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("list registration steps by session id"))

		err = closedStepRepo.UpdateStatus(context.Background(), "session-closed", domain.StepKeyUserCreateProfile, domain.StepStatusCompleted)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("update registration step status"))
	})
})

func startScyllaContainer(ctx context.Context, keyspace string) (testcontainers.Container, config.ScyllaConfig) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "scylladb/scylla:6.1",
			ExposedPorts: []string{"9042/tcp"},
			Cmd:          []string{"--smp", "1", "--memory", "512M", "--overprovisioned", "1"},
			WaitingFor:   wait.ForListeningPort("9042/tcp").WithStartupTimeout(3 * time.Minute),
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

func truncateScyllaTables(session *gocql.Session) {
	for _, query := range []string{
		"TRUNCATE registration_steps",
		"TRUNCATE registration_sessions",
		"TRUNCATE registration_sessions_by_email",
		"TRUNCATE registration_sessions_by_username",
	} {
		Expect(session.Query(query).Exec()).To(Succeed(), fmt.Sprintf("failed to run %s", query))
	}
}

func suiteLogger() logging.Logger {
	logger, err := logging.New("registration-saga-service", "test", "debug")
	Expect(err).NotTo(HaveOccurred())
	return logger
}

func openTempScyllaSession(prefix string) *gocql.Session {
	cfg := repoSuiteCfg
	cfg.Keyspace = fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())

	session, err := pkgscylla.ConnectAndEnsureSchema(cfg, suiteLogger())
	Expect(err).NotTo(HaveOccurred())

	return session
}
