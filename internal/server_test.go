package internal_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"context"
	"google.golang.org/grpc/metadata"
)

var _ = Describe("Server", func() {

	var (
		sut      pub.PublisherServer
		settings Settings
	)

	BeforeEach(func() {
		sut = NewServer()

		settings = Settings{
			Server:   "localhost:1433",
			Database: "w3",
			Auth:     AuthTypeSQL,
			Username: "sa",
			Password: "n5o_ADMIN",
		}
	})

	Describe("Validate settings", func() {

		It("Should error if server is not set", func() {
			settings.Server = ""
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should error if database is not set", func() {
			settings.Database = ""
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should error if auth is not set", func() {
			settings.Auth = ""
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should error if auth is sql and username is not set", func() {
			settings.Auth = AuthTypeSQL
			settings.Username = ""
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should error if auth is sql and password is not set", func() {
			settings.Auth = AuthTypeSQL
			settings.Password = ""
			Expect(settings.Validate()).ToNot(Succeed())
		})
		It("Should succeed if settings are valid", func() {
			Expect(settings.Validate()).To(Succeed())
		})

	})

	Describe("Connect", func() {

		It("should succeed when connection is valid", func(){
			_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
			Expect(err).ToNot(HaveOccurred())
		})

		It("should error when connection is valid", func(){
			settings.Username = "a"
			_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
			Expect(err).To(HaveOccurred())
		})

	})

	Describe("DiscoverShapes", func() {

	})

	Describe("PublishStream", func() {

	})

	Describe("Disconnect", func() {

	})

})

type publisherStream struct {
	records []*pub.Record
	err     error
}

func (p *publisherStream) Send(record *pub.Record) error {
	p.records = append(p.records, record)
	return nil
}

func (publisherStream) SetHeader(metadata.MD) error {
	panic("implement me")
}

func (publisherStream) SendHeader(metadata.MD) error {
	panic("implement me")
}

func (publisherStream) SetTrailer(metadata.MD) {
	panic("implement me")
}

func (publisherStream) Context() context.Context {
	panic("implement me")
}

func (publisherStream) SendMsg(m interface{}) error {
	panic("implement me")
}

func (publisherStream) RecvMsg(m interface{}) error {
	panic("implement me")
}
