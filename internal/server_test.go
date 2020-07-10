package internal_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
	"io"
	"time"
)

var discoverShape = func(sut pub.PublisherServer, schema *pub.Schema) *pub.Schema {
	response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
		Mode:       pub.DiscoverSchemasRequest_REFRESH,
		SampleSize: 0,
		ToRefresh: []*pub.Schema{
			schema,
		},
	})
	Expect(err).ToNot(HaveOccurred())
	var out *pub.Schema
	for _, s := range response.Schemas {
		if s.Id == schema.Id {
			out = s
		}
	}
	Expect(out).ToNot(BeNil(), "should have discovered requested schema %q in %+v", schema.Id, response.Schemas)
	return out
}

var _ = Describe("Server", func() {

	var (
		sut      pub.PublisherServer
		settings Settings
	)

	BeforeEach(func() {

		log := GetLogger()

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

	Describe("wsarecv error handling", func() {

		It("should extract IP from error", func() {
			actual := ExtractIPFromWsarecvErr("rpc error: code = Unknown desc = All attempts fail: #1: read tcp 10.11.0.105:54695->10.11.0.6:1433: wsarecv: An existing connection was forcibly closed by the")
			Expect(actual).To(Equal("10.11.0.6"))
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

				response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
					Mode: pub.DiscoverSchemasRequest_ALL,
				})
				Expect(err).ToNot(HaveOccurred())

				shapes := response.Schemas

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

				Expect(shapes).To(HaveLen(len(allSchemaIDs)), "only tables and views should be returned")
			})

			Describe("shape details", func() {
				var agents *pub.Schema
				BeforeEach(func() {
					response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
						Mode:       pub.DiscoverSchemasRequest_ALL,
						SampleSize: 2,
					})
					Expect(err).ToNot(HaveOccurred())
					for _, s := range response.Schemas {
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

				It("should not include count", func() {
					Expect(agents.Count).To(Equal(&pub.Count{
						Kind: pub.Count_UNAVAILABLE,
					}))
				})

				It("should include sample", func() {
					Expect(agents.Sample).To(HaveLen(2))
				})
			})
		})

		Describe("when mode is REFRESH", func() {

			It("should update shape", func() {

				refreshShape := &pub.Schema{
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

				response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
					Mode:      pub.DiscoverSchemasRequest_REFRESH,
					ToRefresh: []*pub.Schema{refreshShape},
				})
				Expect(err).ToNot(HaveOccurred())
				shapes := response.Schemas
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

			Describe("when shape has query defined", func() {
				It("should update shape", func() {

					refreshShape := &pub.Schema{
						Id:    "agent_names",
						Name:  "Agent Names",
						Query: "SELECT AGENT_CODE, AGENT_NAME AS Name FROM Agents",
					}

					response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
						Mode:       pub.DiscoverSchemasRequest_REFRESH,
						ToRefresh:  []*pub.Schema{refreshShape},
						SampleSize: 5,
					})
					Expect(err).ToNot(HaveOccurred())
					shapes := response.Schemas
					Expect(shapes).To(HaveLen(1), "only requested shape should be returned")

					shape := shapes[0]
					properties := shape.Properties
					Expect(properties).To(ContainElement(&pub.Property{
						Id:           "[AGENT_CODE]",
						Name:         "AGENT_CODE",
						Type:         pub.PropertyType_STRING,
						TypeAtSource: "char(4)",
						IsKey:        true,
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

					Expect(shape.Sample).To(HaveLen(5))
				})
			})

			Describe("when shape has invalid query defined", func() {
				It("should return useful error", func() {

					refreshShape := &pub.Schema{
						Id:    "bogus_table",
						Name:  "Bogus Data",
						Query: "SELECT * FROM Bogus",
					}

					resp, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
						Mode:       pub.DiscoverSchemasRequest_REFRESH,
						ToRefresh:  []*pub.Schema{refreshShape},
						SampleSize: 5,
					})
					Expect(err).ToNot(HaveOccurred())
					Expect(resp.Schemas).To(HaveLen(1))
					shape := resp.Schemas[0]
					Expect(shape.Errors).To(ContainElement(ContainSubstring("Invalid object name")))
				})
			})
		})

	})

	Describe("PublishStream", func() {

		BeforeEach(func() {
			Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
		})

		Describe("pre and post publish queries", func() {

			var req *pub.ReadRequest

			setup := func(settings Settings) {
				var prepost *pub.Schema
				_, err := sut.Connect(context.Background(), pub.NewConnectRequest(settings))
				Expect(err).ToNot(HaveOccurred())

				response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
					Mode:       pub.DiscoverSchemasRequest_ALL,
					SampleSize: 2,
				})
				Expect(err).ToNot(HaveOccurred())
				for _, s := range response.Schemas {
					if s.Id == "[PrePost]" {
						prepost = s
					}
				}
				Expect(prepost).ToNot(BeNil())
				req = &pub.ReadRequest{
					Schema: prepost,
				}

				Expect(db.Exec("DELETE FROM w3.dbo.PrePost")).ToNot(BeNil())
				Expect(db.Exec("INSERT INTO w3.dbo.PrePost VALUES ('placeholder')")).ToNot(BeNil())
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

				row := db.QueryRow("SELECT * FROM w3.dbo.PrePost WHERE Message = 'post'")
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

				row := db.QueryRow("SELECT * FROM w3.dbo.PrePost WHERE Message = 'post'")
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

			var req *pub.ReadRequest
			BeforeEach(func() {
				var agents *pub.Schema

				response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
					Mode:       pub.DiscoverSchemasRequest_ALL,
					SampleSize: 2,
				})
				Expect(err).ToNot(HaveOccurred())
				for _, s := range response.Schemas {
					if s.Id == "[Agents]" {
						agents = s
					}
				}
				Expect(agents).ToNot(BeNil())
				req = &pub.ReadRequest{
					Schema: agents,
				}
			})

			It("should publish all when unfiltered", func() {
				stream := new(publisherStream)
				Expect(sut.PublishStream(req, stream)).To(Succeed())
				Expect(stream.err).ToNot(HaveOccurred())
				Expect(stream.records).To(HaveLen(12))

				var alex map[string]interface{}
				var ramasundar map[string]interface{}
				var data []map[string]interface{}
				for _, record := range stream.records {
					var d map[string]interface{}
					Expect(json.Unmarshal([]byte(record.DataJson), &d)).To(Succeed())
					data = append(data, d)
					switch d["[AGENT_NAME]"] {
					case "Alex":
						alex = d
					case "Ramasundar":
						ramasundar = d
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

				Expect(ramasundar).ToNot(BeNil(), "should find ramusander even though it has a null value")
				Expect(ramasundar).To(And(
					HaveKeyWithValue("[COMMISSION]", BeNil()),
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

			var req *pub.ReadRequest
			BeforeEach(func() {
				var types *pub.Schema

				response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
					Mode:       pub.DiscoverSchemasRequest_ALL,
					SampleSize: 2,
				})
				Expect(err).ToNot(HaveOccurred())
				for _, s := range response.Schemas {
					if s.Id == "[Types]" {
						types = s
					}
				}
				Expect(types).ToNot(BeNil())
				req = &pub.ReadRequest{
					Schema: types,
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

					_, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{})
					Expect(err).To(MatchError(ContainSubstring("not connected")))

					err = sut.PublishStream(&pub.ReadRequest{}, nil)
					Expect(err).To(MatchError(ContainSubstring("not connected")))
				})

			})

		})
	})

	Describe("Write Backs", func() {

		BeforeEach(func() {
			Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
		})

		Describe("ConfigureWrite", func() {

			var req *pub.ConfigureWriteRequest
			BeforeEach(func() {
				req = &pub.ConfigureWriteRequest{}
			})

			It("should return a json form schema on the first call", func() {
				response, err := sut.ConfigureWrite(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.Form).ToNot(BeNil())
				Expect(response.Form.SchemaJson).ToNot(BeNil())
				Expect(response.Form.UiJson).ToNot(BeNil())
				Expect(response.Schema).To(BeNil())
			})

			It("should return a schema when a valid stored procedure is input", func() {
				req.Form = &pub.ConfigurationFormRequest{
					DataJson: `{"storedProcedure":"InsertIntoTypes"}`,
				}

				response, err := sut.ConfigureWrite(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.Form).ToNot(BeNil())
				Expect(response.Schema).ToNot(BeNil())

				Expect(response.Schema.Id).To(Equal("InsertIntoTypes"))
				Expect(response.Schema.Query).To(Equal("InsertIntoTypes"))
				Expect(response.Schema.Properties).To(HaveLen(24))
				Expect(response.Schema.Properties[0].Id).To(Equal("int"))
				Expect(response.Schema.Properties[1].Id).To(Equal("bigint"))
				Expect(response.Schema.Properties[2].Id).To(Equal("numeric"))
				Expect(response.Schema.Properties[2].Type).To(Equal(pub.PropertyType_DECIMAL))
			})

			It("should return a schema when a valid stored procedure with schema is input", func() {
				req.Form = &pub.ConfigurationFormRequest{
					DataJson: `{"storedProcedure":"dev.TEST"}`,
				}

				response, err := sut.ConfigureWrite(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.Form).ToNot(BeNil())
				Expect(response.Schema).ToNot(BeNil())

				Expect(response.Schema.Id).To(Equal("dev.TEST"))
				Expect(response.Schema.Query).To(Equal("dev.TEST"))
				Expect(response.Schema.Properties).To(HaveLen(3))
				Expect(response.Schema.Properties[0].Id).To(Equal("AgentId"))
				Expect(response.Schema.Properties[1].Id).To(Equal("Name"))
				Expect(response.Schema.Properties[2].Id).To(Equal("Commission"))
				Expect(response.Schema.Properties[2].Type).To(Equal(pub.PropertyType_FLOAT))
			})

			It("should return an error when an invalid stored procedure is input", func() {
				req.Form = &pub.ConfigurationFormRequest{
					DataJson: `{"storedProcedure":"NOT A PROC"}`,
				}

				response, err := sut.ConfigureWrite(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.Form).ToNot(BeNil())
				Expect(response.Schema).ToNot(BeNil())
				Expect(response.Form.Errors).To(HaveLen(1))
				Expect(response.Form.Errors[0]).To(ContainSubstring("stored procedure does not exist"))
			})
		})

		Describe("PrepareWrite", func() {

			var req *pub.PrepareWriteRequest
			BeforeEach(func() {
				req = &pub.PrepareWriteRequest{
					Schema: &pub.Schema{},
					CommitSlaSeconds: 1,
				}
			})

			It("should prepare the plugin to write", func() {
				response, err := sut.PrepareWrite(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())
				Expect(response).ToNot(BeNil())
			})
		})

		Describe("WriteStream", func() {

			var createStream func(map[string]interface{}) *writeStream
			var req *pub.PrepareWriteRequest
			BeforeEach(func() {
				Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
				req =  &pub.PrepareWriteRequest{
					Schema: &pub.Schema{
						Id: "InsertIntoTypes",
						Query: "InsertIntoTypes",
						Properties: []*pub.Property {
							{
								Id: "int",
								Type: pub.PropertyType_INTEGER,
								TypeAtSource: "int",
							},
							{
								Id: "bigint",
								Type: pub.PropertyType_DECIMAL,
								TypeAtSource: "bigint",
							},
							{
								Id: "numeric",
								Type: pub.PropertyType_DECIMAL,
								TypeAtSource: "numeric(18,5)",
							},
							{
								Id: "bit",
								Type: pub.PropertyType_BOOL,
								TypeAtSource: "bit",
							},
							{
								Id: "smallint",
								Type: pub.PropertyType_INTEGER,
								TypeAtSource: "smallint",
							},
							{
								Id: "decimal",
								Type: pub.PropertyType_DECIMAL,
								TypeAtSource: "decimal(18,4)",
							},
							{
								Id: "smallmoney",
								Type: pub.PropertyType_DECIMAL,
								TypeAtSource: "smallmoney",
							},
							{
								Id: "tinyint",
								Type: pub.PropertyType_INTEGER,
								TypeAtSource: "tinyint",
							},
							{
								Id: "money",
								Type: pub.PropertyType_DECIMAL,
								TypeAtSource: "money",
							},
							{
								Id: "float",
								Type: pub.PropertyType_FLOAT,
								TypeAtSource: "float",
							},
							{
								Id: "real",
								Type: pub.PropertyType_FLOAT,
								TypeAtSource: "real",
							},
							{
								Id: "date",
								Type: pub.PropertyType_DATE,
								TypeAtSource: "date",
							},
							{
								Id: "datetimeoffset",
								Type: pub.PropertyType_STRING,
								TypeAtSource: "datetimeoffset(7)",
							},
							{
								Id: "datetime2",
								Type: pub.PropertyType_DATETIME,
								TypeAtSource: "datetime2(7)",
							},
							{
								Id: "smalldatetime",
								Type: pub.PropertyType_DATETIME,
								TypeAtSource: "smalldatetime",
							},
							{
								Id: "datetime",
								Type: pub.PropertyType_DATETIME,
								TypeAtSource: "datetime",
							},
							{
								Id: "time",
								Type: pub.PropertyType_TIME,
								TypeAtSource: "time(7)",
							},
							{
								Id: "char",
								Type: pub.PropertyType_STRING,
								TypeAtSource: "char(6)",
							},
							{
								Id: "varchar",
								Type: pub.PropertyType_STRING,
								TypeAtSource: "varchar(10)",
							},
							{
								Id: "text",
								Type: pub.PropertyType_TEXT,
								TypeAtSource: "text",
							},
							{
								Id: "nchar",
								Type: pub.PropertyType_STRING,
								TypeAtSource: "nchar(6)",
							},
							{
								Id: "nvarchar",
								Type: pub.PropertyType_STRING,
								TypeAtSource: "nvarchar(10)",
							},
							{
								Id: "ntext",
								Type: pub.PropertyType_STRING,
								TypeAtSource: "ntext",
							},							{
								Id: "uniqueidentifier",
								Type: pub.PropertyType_STRING,
								TypeAtSource: "uniqueidentifier",
							},
						},
					},
					CommitSlaSeconds: 1,
				}

				createStream = func(data map[string]interface{}) *writeStream {

					dataJson, _ := json.Marshal(data)

					records := []*pub.Record{
						{
						DataJson: string(dataJson),
						CorrelationId: "test",
					}}

					stream := &writeStream{
						records: records,
						index: 0,
					}
					return stream
				}

			})

			It("should be able to call a stored procedure to write a record", func() {
				response, err := sut.PrepareWrite(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())
				Expect(response).ToNot(BeNil())

				stream := createStream(map[string]interface{}{
					"int": 43,
					"bigint": "9223372036854775807",
					"numeric": "1234.56780",
					"smallint": 123,
					"decimal": "1234.5678",
					"smallmoney": "12.5600",
					"tinyint":  12,
					"money": "1234.5600",
					"float": 123456.789,
					"real": 123456.789,
					"bit": true,
					"date": "1970-01-01T00:00:00Z",
					"datetimeoffset": "2007-05-08T12:35:29.1234567+12:15",
					"datetime2": "2007-05-08T12:35:29.1234567Z",
					"smalldatetime": "2007-05-08T12:35:00Z",
					"datetime": "2007-05-08T12:35:29.123Z",
					"time": "0001-01-01T12:35:29.123Z",
					"char": "char  ",
					"varchar": "abc",
					"text": "abc",
					"nchar": "nchar ",
					"nvarchar": "nvarchar",
					"ntext": "ntext",
					"binary": base64.StdEncoding.EncodeToString([]byte("abc")),
					"varbinary": base64.StdEncoding.EncodeToString([]byte("cde")),
					"uniqueidentifier": uniqueIdentifierIdSteve,
				})

				Expect(sut.WriteStream(stream)).To(Succeed())

				Eventually(func() []*pub.RecordAck {
					return stream.recordAcks
				}).Should(HaveLen(1))
				Expect(stream.recordAcks[0].CorrelationId).To(Equal("test"))
				Expect(stream.recordAcks[0].Error).To(Equal(""))

				rows, err := db.Query("select * from dbo.Types where [int] = 43")
				Expect(err).ToNot(HaveOccurred())

				var typeStructs []Types
				err = sqlstructs.UnmarshalRows(rows, &typeStructs)
				Expect(err).ToNot(HaveOccurred())
				Expect(typeStructs).To(HaveLen(1))
				// spew.Dump(typeStructs[0])

			})

			It("should get an error if the stored procedure fails to write a record", func() {
				response, err := sut.PrepareWrite(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())
				Expect(response).ToNot(BeNil())

				stream := createStream(map[string]interface{}{
					"int": 44,
					"bit": nil, // this column is not nullable
				})

				Expect(sut.WriteStream(stream)).To(Succeed())

				Eventually(func() []*pub.RecordAck {
					return stream.recordAcks
				}).Should(HaveLen(1))
				Expect(stream.recordAcks[0].CorrelationId).To(Equal("test"))
				Expect(stream.recordAcks[0].Error).ToNot(BeEmpty())

				rows, err := db.Query("select * from dbo.Types where [int] = 44")
				Expect(err).ToNot(HaveOccurred())

				var typeStructs []Types
				err = sqlstructs.UnmarshalRows(rows, &typeStructs)
				Expect(err).ToNot(HaveOccurred())
				Expect(typeStructs).To(HaveLen(0))

			})
		})
	})
})

type Types struct {
	Bigint int `sql:"bigint"`
	
	Bit bool `sql:"bit"`
	Char string `sql:"char"`
	Date time.Time `sql:"date"`
	Datetime time.Time `sql:"datetime"`
	Datetime2 time.Time `sql:"datetime2"`
	DatetimeOffset time.Time `sql:"datetimeoffset"`
	Decimal float64 `sql:"decimal"`
	Float float64 `sql:"float"`
	Int int `sql:"int"`
	Money float64 `sql:"money"`
	Nchar string `sql:"nchar"`
	Ntext string `sql:"ntext"`
	Numeric float64 `sql:"numeric"`
	Nvarchar string `sql:"nvarchar"`
	Real float64 `sql:"real"`
	Smalldatetime time.Time `sql:"smalldatetime"`
	Smallint int `sql:"smallint"`
	Smallmoney float64 `sql:"smallmoney"`
	Text string `sql:"text"`
	Time time.Time `sql:"time"`
	Tinyint int `sql:"tinyint"`
	Varchar string `sql:"varchar"`
}

type writeStream struct {
	records 	[]*pub.Record
	recordAcks 	[]*pub.RecordAck
	index 		int
	err     	error
}

func (p *writeStream) Send(ack *pub.RecordAck) error {
	if p.err != nil {
		return p.err
	}

	p.recordAcks = append(p.recordAcks, ack)
	return nil
}

func (p *writeStream) Recv() (*pub.Record, error) {
	if p.err != nil {
		return nil, p.err
	}

	if len(p.records) > p.index {
		record := p.records[p.index]
		p.index++
		return record, nil
	}

	return nil, io.EOF
}

func (writeStream) SetHeader(metadata.MD) error {
	panic("implement me")
}

func (writeStream) SendHeader(metadata.MD) error {
	panic("implement me")
}

func (writeStream) SetTrailer(metadata.MD) {
	panic("implement me")
}

func (writeStream) Context() context.Context {
	return context.Background()
}

func (writeStream) SendMsg(m interface{}) error {
	panic("implement me")
}

func (writeStream) RecvMsg(m interface{}) error {
	panic("implement me")
}

type publisherStream struct {
	records []*pub.Record
	out     chan *pub.Record
	err     error
}

func (p *publisherStream) Send(record *pub.Record) error {
	if p.err != nil {
		return p.err
	}
	p.records = append(p.records, record)
	if p.out != nil {
		p.out <- record
	}
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

func unmarshallString(jsn string, out interface{}) {
	err := json.Unmarshal([]byte(jsn), &out)
	Expect(err).ToNot(HaveOccurred())
}
