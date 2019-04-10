package templates_test

import (
	"log"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTemplates(t *testing.T) {
	RegisterFailHandler(Fail)
	log.SetOutput(GinkgoWriter)
	RunSpecs(t, "Templates Suite")
}
