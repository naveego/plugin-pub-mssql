package internal_test

import (
	. "github.com/naveego/plugin-pub-mssql/internal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func testMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {
		return PublishHandlerFunc(func(item PublishRequest) error{
			return handler.Handle(item)
		})
	}
}

var _ = Describe("PublishRequestHandler", func() {
	It("should label errors as expected", func() {
		handler := PublishHandlerFunc(func(item PublishRequest) error{
			return errors.New("expected")
		})
		sut := ApplyMiddleware(handler, testMiddleware())

		err := sut.Handle(PublishRequest{})
		Expect(err).To(HaveOccurred())
		msg := err.Error()
		Expect(msg).To(ContainSubstring("expected"))
		Expect(msg).To(ContainSubstring("testMiddleware:"))
	})
})
