package main

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/fx"
)

func TestMainPackage(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main Suite")
}

type fakeRunner struct {
	ran *bool
}

func (r fakeRunner) Run() {
	*r.ran = true
}

var _ = Describe("main", func() {
	It("builds and runs the fx app", func() {
		previousNewApp := newApp
		previousRunApp := runApp
		defer func() {
			newApp = previousNewApp
			runApp = previousRunApp
		}()

		ran := false
		var optionCount int
		newApp = func(opts ...fx.Option) *fx.App {
			optionCount = len(opts)
			return fx.New(fx.NopLogger)
		}
		runApp = func(*fx.App) {
			fakeRunner{ran: &ran}.Run()
		}

		main()

		Expect(ran).To(BeTrue())
		Expect(optionCount).To(Equal(8))
	})
})
