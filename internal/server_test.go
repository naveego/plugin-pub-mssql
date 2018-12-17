package internal_test

import (
	"github.com/hashicorp/go-hclog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"context"
	"google.golang.org/grpc/metadata"
	"encoding/json"
	"encoding/base64"
	"github.com/pkg/errors"
	"fmt"
)

var _ = Describe("Server", func() {

	var (
		sut      pub.PublisherServer
		settings Settings
	)

	BeforeEach(func() {

		log := hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Trace,
			Output:     GinkgoWriter,
			JSONFormat: true,
		})

		sut = NewServer(log)

		settings = *GetTestSettings()
	})

	Describe("Connect", func() {

		It("should succeed when connection is valid", func() {
			_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
			Expect(err).ToNot(HaveOccurred())
		})

		It("should error when connection is invalid", func() {
			settings.Username = "a"
			_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
			Expect(err).To(HaveOccurred())
		})

		It("should error when settings are malformed", func() {
			_, err := sut.Connect(context.Background(), &pub.ConnectRequest{SettingsJson: "{"})
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
				Expect(ids).To(ContainElement("[Types]"), "dbo schema should be stripped")
				Expect(ids).To(ContainElement("[PrePost]"), "dbo schema should be stripped")
				Expect(ids).To(ContainElement("[Agents]"), "dbo schema should be stripped")
				Expect(ids).To(ContainElement("[Customers]"), "dbo schema should be stripped")
				Expect(ids).To(ContainElement("[fact].[Orders]"), "schema should be included if not dbo")
				Expect(ids).To(ContainElement("[Agents per Working Area]"), "views should be included")

				Expect(shapes).To(HaveLen(6), "only tables and views should be returned")
			})

			Describe("shape details", func() {
				var agents *pub.Shape
				BeforeEach(func() {
					response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
						Mode:       pub.DiscoverShapesRequest_ALL,
						SampleSize: 2,
					})
					Expect(err).ToNot(HaveOccurred())
					for _, s := range response.Shapes {
						if s.Id == "[Agents]" {
							agents = s
						}
					}
					Expect(agents).ToNot(BeNil())

					agentsJSON, _ := json.Marshal(agents)
					fmt.Println("Agents JSON:", string(agentsJSON))
				})

				It("should include properties", func() {
					properties := agents.Properties

					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[AGENT_CODE]",
						Name:         "AGENT_CODE",
						Type:         pub.PropertyType_STRING,
						TypeAtSource: "char(4)",
						IsKey:        true,
						IsNullable:   false,
					}))
					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[COMMISSION]",
						Name:         "COMMISSION",
						Type:         pub.PropertyType_FLOAT,
						TypeAtSource: "float",
						IsNullable:   true,
					}))
					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[BIOGRAPHY]",
						Name:         "BIOGRAPHY",
						Type:         pub.PropertyType_TEXT,
						TypeAtSource: "varchar(max)",
						IsNullable:   true,
					}))
				})

				It("should include count", func() {
					Expect(agents.Count).To(Equal(&pub.Count{
						Kind:  pub.Count_EXACT,
						Value: 12,
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
							IsNullable:   true,
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
					TypeAtSource: "varchar(35)",
					IsNullable:   true,
				}))
				Expect(properties).To(ContainElement(&pub.Property{
					Id:           "[COUNT]",
					Name:         "COUNT",
					Type:         pub.PropertyType_INTEGER,
					TypeAtSource: "int",
					IsNullable:   true,
				}))
			})

			Describe("when shape has query defined", func(){
				It("should update shape", func() {

					refreshShape := &pub.Shape{
						Id:   "agent_names",
						Name: "Agent Names",
						Query: "SELECT AGENT_CODE, AGENT_NAME AS Name FROM Agents",
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
						Id:           "[AGENT_CODE]",
						Name:         "AGENT_CODE",
						Type:         pub.PropertyType_STRING,
						TypeAtSource: "char(4)",
						IsKey:		  true,
					}))
					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[Name]",
						Name:         "Name",
						Type:         pub.PropertyType_STRING,
						TypeAtSource: "varchar(40)",
						IsNullable:   true,
					}))

					Expect(shape.Count).To(Equal(&pub.Count{
						Kind:  pub.Count_EXACT,
						Value: 12,
					}))
				})
			})


		})

		Describe("PublishStream", func() {

			Describe("pre and post publish queries", func() {

				var req *pub.PublishRequest

				setup := func(settings Settings) {
					var prepost *pub.Shape
					_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
					Expect(err).ToNot(HaveOccurred())

					response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
						Mode:       pub.DiscoverShapesRequest_ALL,
						SampleSize: 2,
					})
					Expect(err).ToNot(HaveOccurred())
					for _, s := range response.Shapes {
						if s.Id == "[PrePost]" {
							prepost = s
						}
					}
					Expect(prepost).ToNot(BeNil())
					req = &pub.PublishRequest{
						Shape: prepost,
					}

					Expect(db.Exec("delete from w3.dbo.PrePost")).ToNot(BeNil())
					Expect(db.Exec("insert into w3.dbo.PrePost values ('placeholder')")).ToNot(BeNil())
				}

				It("should run pre-publish query", func() {
					settings.PrePublishQuery = "INSERT INTO w3.dbo.PrePost VALUES ('pre')"
					setup(settings)

					stream := new(publisherStream)
					Expect(sut.PublishStream(req, stream)).To(Succeed())
					Expect(stream.err).ToNot(HaveOccurred())
					Expect(stream.records).To(
						ContainElement(
							WithTransform(func(e *pub.Record) string { return e.DataJson },
								ContainSubstring("pre"))))
				})

				It("should run post-publish query", func() {
					settings.PostPublishQuery = "INSERT INTO w3.dbo.PrePost VALUES ('post')"
					setup(settings)
					stream := new(publisherStream)
					Expect(sut.PublishStream(req, stream)).To(Succeed())

					row := db.QueryRow("select * from w3.dbo.PrePost where Message = 'post'")
					var msg string
					Expect(row.Scan(&msg)).To(Succeed())
					Expect(msg).To(Equal("post"))
				})

				It("should run post-publish query even if publish fails", func() {
					settings.PostPublishQuery = "INSERT INTO w3.dbo.PrePost VALUES ('post')"
					setup(settings)
					stream := new(publisherStream)
					stream.err = errors.New("expected")

					Expect(sut.PublishStream(req, stream)).To(MatchError(ContainSubstring("expected")))

					row := db.QueryRow("select * from w3.dbo.PrePost where Message = 'post'")
					var msg string
					Expect(row.Scan(&msg)).To(Succeed())
					Expect(msg).To(Equal("post"))
				})

				It("should combine post-publish query error with publish error if publish fails", func() {
					settings.PostPublishQuery = "INSERT INTO w3.dbo.PrePost 'invalid syntax'"
					setup(settings)
					stream := new(publisherStream)
					stream.err = errors.New("expected")

					Expect(sut.PublishStream(req, stream)).To(
						MatchError(
							And(
								ContainSubstring("expected"),
								ContainSubstring("invalid"),
							)))
				})
			})

			Describe("filtering", func() {

				var req *pub.PublishRequest
				BeforeEach(func() {
					var agents *pub.Shape

					response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
						Mode:       pub.DiscoverShapesRequest_ALL,
						SampleSize: 2,
					})
					Expect(err).ToNot(HaveOccurred())
					for _, s := range response.Shapes {
						if s.Id == "[Agents]" {
							agents = s
						}
					}
					Expect(agents).ToNot(BeNil())
					req = &pub.PublishRequest{
						Shape: agents,
					}
				})

				It("should publish all when unfiltered", func() {
					stream := new(publisherStream)
					Expect(sut.PublishStream(req, stream)).To(Succeed())
					Expect(stream.err).ToNot(HaveOccurred())
					Expect(stream.records).To(HaveLen(12))

					var alex map[string]interface{}
					var data []map[string]interface{}
					for _, record := range stream.records {
						var d map[string]interface{}
						Expect(json.Unmarshal([]byte(record.DataJson), &d)).To(Succeed())
						data = append(data, d)
						if d["[AGENT_NAME]"] == "Alex" {
							alex = d
						}
					}
					Expect(alex).ToNot(BeNil(), "should find Alex (code==A003)")

					Expect(alex).To(And(
						HaveKeyWithValue("[AGENT_CODE]", "A003"),
						HaveKeyWithValue("[AGENT_NAME]", "Alex"),
						HaveKeyWithValue("[WORKING_AREA]", "London"),
						HaveKeyWithValue("[COMMISSION]", float64(0.13)),
						HaveKeyWithValue("[PHONE_NO]", "075-12458969"),
						HaveKeyWithValue("[UPDATED_AT]", "1970-01-02T00:00:00Z"),
						HaveKeyWithValue("[BIOGRAPHY]", ""),
					))
				})

				It("should filter on equality", func() {
					stream := new(publisherStream)
					req.Filters = []*pub.PublishFilter{
						{
							Kind:       pub.PublishFilter_EQUALS,
							PropertyId: "[AGENT_CODE]",
							Value:      "A003",
						},
					}
					Expect(sut.PublishStream(req, stream)).To(Succeed())
					Expect(stream.err).ToNot(HaveOccurred())
					Expect(stream.records).To(HaveLen(1))
					Expect(stream.records[0].DataJson).To(ContainSubstring("Alex"))
				})

				It("should filter on GREATER_THAN", func() {
					stream := new(publisherStream)
					req.Filters = []*pub.PublishFilter{
						{
							Kind:       pub.PublishFilter_GREATER_THAN,
							PropertyId: "[UPDATED_AT]",
							Value:      "1970-01-02T00:00:00Z",
						},
					}
					Expect(sut.PublishStream(req, stream)).To(Succeed())
					Expect(stream.err).ToNot(HaveOccurred())
					Expect(stream.records).To(HaveLen(7))
				})
				It("should filter on LESS_THAN", func() {
					stream := new(publisherStream)
					req.Filters = []*pub.PublishFilter{
						{
							Kind:       pub.PublishFilter_LESS_THAN,
							PropertyId: "[COMMISSION]",
							Value:      "0.12",
						},
					}
					Expect(sut.PublishStream(req, stream)).To(Succeed())
					Expect(stream.err).ToNot(HaveOccurred())
					Expect(stream.records).To(HaveLen(2))
				})
			})

			Describe("typing", func() {

				var req *pub.PublishRequest
				BeforeEach(func() {
					var types *pub.Shape

					response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
						Mode:       pub.DiscoverShapesRequest_ALL,
						SampleSize: 2,
					})
					Expect(err).ToNot(HaveOccurred())
					for _, s := range response.Shapes {
						if s.Id == "[Types]" {
							types = s
						}
					}
					Expect(types).ToNot(BeNil())
					req = &pub.PublishRequest{
						Shape: types,
					}
				})

				It("should publish record with all data in correct format", func() {
					stream := new(publisherStream)
					Expect(sut.PublishStream(req, stream)).To(Succeed())
					Expect(stream.err).ToNot(HaveOccurred())
					Expect(stream.records).To(HaveLen(1))
					record := stream.records[0]
					var data map[string]interface{}
					Expect(json.Unmarshal([]byte(record.DataJson), &data)).To(Succeed())

					Expect(data).To(And(
						HaveKeyWithValue("[int]", BeNumerically("==", 42)),
						HaveKeyWithValue("[bigint]", Equal("9223372036854775807")),
						HaveKeyWithValue("[numeric]", Equal("1234.56780")),
						HaveKeyWithValue("[smallint]", BeNumerically("==", 123)),
						HaveKeyWithValue("[decimal]", Equal("1234.5678")),
						HaveKeyWithValue("[smallmoney]", Equal("12.5600")),
						HaveKeyWithValue("[tinyint]", BeNumerically("==", 12)),
						HaveKeyWithValue("[money]", Equal("1234.5600")),
						HaveKeyWithValue("[float]", BeNumerically("~", 123456.789, 1E8)),
						HaveKeyWithValue("[real]", BeNumerically("~", 123456.789, 1E8)),
						HaveKeyWithValue("[bit]", true),
						HaveKeyWithValue("[date]", "1970-01-01T00:00:00Z"),
						HaveKeyWithValue("[datetimeoffset]", "2007-05-08T12:35:29.1234567+12:15", ),
						HaveKeyWithValue("[datetime2]", "2007-05-08T12:35:29.1234567Z", ),
						HaveKeyWithValue("[smalldatetime]", "2007-05-08T12:35:00Z"),
						HaveKeyWithValue("[datetime]", "2007-05-08T12:35:29.123Z"),
						HaveKeyWithValue("[time]", "0001-01-01T12:35:29.123Z"),
						HaveKeyWithValue("[char]", "char  "),
						HaveKeyWithValue("[varchar]", "abc"),
						HaveKeyWithValue("[text]", "abc"),
						HaveKeyWithValue("[nchar]", "nchar "),
						HaveKeyWithValue("[nvarchar]", "nvarchar"),
						HaveKeyWithValue("[ntext]", "ntext"),
						HaveKeyWithValue("[binary]", base64.StdEncoding.EncodeToString([]byte("abc"))),
						HaveKeyWithValue("[varbinary]", base64.StdEncoding.EncodeToString([]byte("cde"))),
					))

				})

				Describe("Disconnect", func() {

					It("should not be connected after disconnect", func() {
						Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())

						_, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{})
						Expect(err).To(MatchError(ContainSubstring("not connected")))

						err = sut.PublishStream(&pub.PublishRequest{}, nil)
						Expect(err).To(MatchError(ContainSubstring("not connected")))
					})

				})

			})
		})
	})
})

type publisherStream struct {
	records []*pub.Record
	err     error
}

func (p *publisherStream) Send(record *pub.Record) error {
	if p.err != nil {
		return p.err
	}
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
