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

		It("should succeed when connection is valid", func() {
			_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
			Expect(err).ToNot(HaveOccurred())
		})

		It("should error when connection is valid", func() {
			settings.Username = "a"
			_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
			Expect(err).To(HaveOccurred())
		})

	})

	Describe("DiscoverShapes", func() {

		BeforeEach(func() {
			Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
		})

		Describe("when mode is ALL", func() {

			It("should get tables and views", func() {

				response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
					Mode: pub.DiscoverShapesRequest_ALL,
				})
				Expect(err).ToNot(HaveOccurred())

				shapes := response.Shapes

				var ids []string
				for _, s := range shapes {
					ids = append(ids, s.Id)
				}
				Expect(ids).To(ContainElement("[Agents]"), "dbo schema should be stripped")
				Expect(ids).To(ContainElement("[Customers]"), "dbo schema should be stripped")
				Expect(ids).To(ContainElement("[fact].[Orders]"), "schema should be included if not dbo")
				Expect(ids).To(ContainElement("[Agents per Working Area]"), "views should be included")

				Expect(shapes).To(HaveLen(4), "only tables and views should be returned")
			})

			Describe("shape details", func(){
				var agents *pub.Shape
				BeforeEach(func(){
					response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
						Mode: pub.DiscoverShapesRequest_ALL,
						SampleSize:2,
					})
					Expect(err).ToNot(HaveOccurred())
					for _, s := range response.Shapes {
						if s.Id == "[Agents]" {
							agents = s
						}
					}
					Expect(agents).ToNot(BeNil())
				})

				It("should include properties", func() {
					properties := agents.Properties

					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[AGENT_CODE]",
						Name:         "AGENT_CODE",
						Type:         pub.PropertyType_STRING,
						TypeAtSource: "char(6)",
						IsKey:        true,
					}))
					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[COMMISSION]",
						Name:         "COMMISSION",
						Type:         pub.PropertyType_NUMBER,
						TypeAtSource: "numeric(10,2)",
					}))
					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[BIOGRAPHY]",
						Name:         "BIOGRAPHY",
						Type:         pub.PropertyType_TEXT,
						TypeAtSource: "varchar(MAX)",
					}))
				})

				It("should include count", func() {
					Expect(agents.Count).To(Equal(&pub.Count{
						Kind:pub.Count_EXACT,
						Value:12,
					}))
				})

				It("should include sample", func() {
					Expect(agents.Sample).To(HaveLen(2))
				})
			})


		})

		Describe("when mode is REFRESH", func() {

			It("should update shape", func() {

				refreshShape := &pub.Shape{
					Id:   "[Agents per Working Area]",
					Name: "Agents per Working Area",
					Properties: []*pub.Property{
						{
							Id:           "[COUNT]",
							Name:         "COUNT",
							Type:         pub.PropertyType_STRING,
							TypeAtSource: "int",
						},
					},
				}

				response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
					Mode:      pub.DiscoverShapesRequest_REFRESH,
					ToRefresh: []*pub.Shape{refreshShape},
				})
				Expect(err).ToNot(HaveOccurred())
				shapes := response.Shapes
				Expect(shapes).To(HaveLen(1), "only requested shape should be returned")

				shape := shapes[0]
				properties := shape.Properties
				Expect(properties).To(ContainElement(&pub.Property{
					Id:           "[WORKING_AREA]",
					Name:         "WORKING_AREA",
					Type:         pub.PropertyType_STRING,
					TypeAtSource: "char(35)",
				}))
				Expect(properties).To(ContainElement(&pub.Property{
					Id:           "[COUNT]",
					Name:         "COUNT",
					Type:         pub.PropertyType_NUMBER,
					TypeAtSource: "int",
				}))
			})
		})

		Describe("PublishStream", func() {

			// record := agents.Sample[0]
			// var data map[string]interface{}
			// Expect(json.Unmarshal([]byte(record.DataJson), &data)).To(Succeed())
			// Expect(data).To(BeEquivalentTo(map[]))

		})

		Describe("Disconnect", func() {

		})

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
