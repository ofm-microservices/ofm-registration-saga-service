package scylla

import (
	"errors"
	"testing"

	"github.com/gocql/gocql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStorageScylla(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Storage Scylla Suite")
}

var _ = Describe("Scylla storage helpers", func() {
	It("parses consistency levels", func() {
		Expect(parseConsistency("one")).To(Equal(gocql.One))
		Expect(parseConsistency("localquorum")).To(Equal(gocql.LocalQuorum))
		Expect(parseConsistency("all")).To(Equal(gocql.All))
		Expect(parseConsistency("unknown")).To(Equal(gocql.Quorum))
	})

	It("wraps bootstrap errors", func() {
		err := errors.New("boom")
		Expect(WrapCreateClusterSessionError(err)).To(MatchError(ContainSubstring("create scylla session")))
		Expect(WrapEnsureSchemaError(err)).To(MatchError(ContainSubstring("ensure scylla schema")))
	})
})
