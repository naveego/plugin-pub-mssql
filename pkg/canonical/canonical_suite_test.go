package canonical_test

import (
	"github.com/naveego/plugin-pub-mssql/pkg/canonical"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestCanonical(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Canonical Suite")
}

var _ = DescribeTable("should canonicalize strings", func(inputs []string, expecteds []string) {
	sut := canonical.Strings(canonical.ToAlphanumeric, canonical.ToPascalCase)
	for i := range inputs {
		Expect(sut.Canonicalize(inputs[i])).To(Equal(expecteds[i]), "input was %q @ %d", inputs[i], i)
	}
},
Entry("simple", []string{
	"abc",
	"A B C",
	"A B C",
	"A B C",
	"Alpha Beta Gamma",
	"Alpha-beta-gamma",
	"123",
	"A-b-c-1-2-3",
},
[]string{
	"Abc",
	"ABC_2",
	"ABC_3",
	"ABC_4",
	"AlphaBetaGamma",
	"AlphaBetaGamma_2",
	"123",
	"ABC123",
}))