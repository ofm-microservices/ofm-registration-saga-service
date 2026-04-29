package scylla

import (
	"errors"
	"testing"

	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRepository(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Infra Scylla Suite")
}

var _ = Describe("Scylla repositories", func() {
	It("validates nil sessions", func() {
		sessionRepo, err := NewSessionRepository(nil)
		Expect(sessionRepo).To(BeNil())
		Expect(err).To(MatchError("scylla session is nil"))

		stepRepo, err := NewStepRepository(nil)
		Expect(stepRepo).To(BeNil())
		Expect(err).To(MatchError("scylla session is nil"))
	})

	It("constructs repositories with a session pointer", func() {
		session := &gocql.Session{}

		sessionRepo, err := NewSessionRepository(session)
		Expect(err).NotTo(HaveOccurred())
		Expect(sessionRepo).NotTo(BeNil())

		stepRepo, err := NewStepRepository(session)
		Expect(err).NotTo(HaveOccurred())
		Expect(stepRepo).NotTo(BeNil())
	})

	It("wraps repository errors", func() {
		err := errors.New("boom")
		Expect(WrapCreateSessionError(err)).To(MatchError(ContainSubstring("create registration session")))
		Expect(WrapGetSessionByIDError(err)).To(MatchError(ContainSubstring("get registration session by id")))
		Expect(WrapUpdateSessionStatusError(err)).To(MatchError(ContainSubstring("update registration session status")))
		Expect(WrapGetSessionByEmailError(err)).To(MatchError(ContainSubstring("get registration session by email")))
		Expect(WrapGetSessionByUsernameError(err)).To(MatchError(ContainSubstring("get registration session by username")))
		Expect(WrapCreateStepError(err)).To(MatchError(ContainSubstring("create registration step")))
		Expect(WrapGetStepByKeyError(err)).To(MatchError(ContainSubstring("get registration step by key")))
		Expect(WrapListStepsBySessionIDError(err)).To(MatchError(ContainSubstring("list registration steps by session id")))
		Expect(WrapUpdateStepStatusError(err)).To(MatchError(ContainSubstring("update registration step status")))
	})
})
