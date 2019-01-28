package internal_test

import (
	"context"
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConfigureRealTime", func() {

	var (
		sut      pub.PublisherServer
		settings Settings
	)

	BeforeEach(func() {

		sut = NewServer(log)

		settings = *GetTestSettings()

	})

	BeforeEach(func() {
		Expect(sut.Configure(context.Background(), &pub.ConfigureRequest{
			LogLevel:           pub.LogLevel_Trace,
			PermanentDirectory: "./data",
			TemporaryDirectory: "./temp",
		}))
		Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
	})

	AfterEach(func() {
		Expect(sut.Disconnect(context.Background(), new(pub.DisconnectRequest))).ToNot(BeNil())
	})
	Describe("ConfigureRealTime", func() {

		var configureRealTime = func(schemaID string, settings RealTimeSettings) *pub.ConfigureRealTimeResponse {
			shape := discoverShape(sut, &pub.Shape{
				Id: schemaID,
			})
			req := (&pub.ConfigureRealTimeRequest{
				Shape: shape,
			}).WithData(settings)
			resp, err := sut.ConfigureRealTime(context.Background(), req)
			Expect(err).ToNot(HaveOccurred())
			return resp
		}

		BeforeEach(func() {
			Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
		})

		Describe("when schema is table", func() {
			Describe("when table has change tracking enabled", func() {
				It("should return a form schema with no properties", func() {
					resp := configureRealTime("[RealTime]", RealTimeSettings{})
					jsonSchemaForForm := resp.GetJSONSchemaForForm()
					Expect(jsonSchemaForForm.Properties).To(BeEmpty())
					Expect(jsonSchemaForForm.Description).ToNot(BeEmpty())
				})
			})
			Describe("when table does not have change tracking enabled", func() {
				It("should return an empty form schema with an error", func() {
					resp := configureRealTime("[Agents]", RealTimeSettings{})
					jsonSchemaForForm := resp.GetJSONSchemaForForm()
					Expect(jsonSchemaForForm.Properties).To(BeEmpty())
					Expect(resp.Form.Errors).To(ContainElement(ContainSubstring("Table does not have change tracking enabled.")))
				})
			})
		})

		Describe("when schema is a view", func() {

			It("should have error if table does not have change tracking enabled", func() {

				resp := configureRealTime("RealTimeDuplicateView", RealTimeSettings{
					Tables: []RealTimeTableSettings{
						{SchemaID: "[Customers]"},
					},
				})
				dataErrors := resp.GetDataErrorsJSONAsMap()
				Expect(dataErrors).To(HaveKeyWithValue("tables",
					ContainElement(HaveKeyWithValue("tableName", ContainSubstring("Table does not have change tracking enabled.")))))

			})

			It("should round trip settings", func() {
				var actual RealTimeSettings
				expectedSettings := RealTimeSettings{
					Tables: []RealTimeTableSettings{
						{SchemaID: "[RealTime]"},
					},
				}
				resp := configureRealTime("RealTimeDirectView", expectedSettings)
				unmarshallString(resp.Form.DataJson, &actual)
				Expect(actual).To(BeEquivalentTo(expectedSettings))
			})

			Describe("query validation", func() {

				It("should detect invalid query", func() {
					expectedSettings := RealTimeSettings{
						Tables: []RealTimeTableSettings{
							{
								SchemaID: "[RealTime]",
								Query: `SELECT [RealTimeDuplicateView].id as [Source.id], [RealTime].id as [Dep.id]
								FROM RealTimeDuplicateView
								JOIN RealTime on [RealTimeDuplicateView].id = [RealTime].id`,
							},
						},
					}
					var errs map[string]interface{}
					resp := configureRealTime("RealTimeDirectView", expectedSettings)
					unmarshallString(resp.Form.DataErrorsJson, &errs)
					Expect(errs).To(HaveKeyWithValue("tables",
						ContainElement(HaveKeyWithValue("query", And(
							ContainSubstring("[Schema.id]"),
							ContainSubstring("[Dependency.id]"),
						)))))

				})

				It("should detect valid query", func() {
					expectedSettings := RealTimeSettings{
						Tables: []RealTimeTableSettings{
							{
								SchemaID: "[RealTime]",
								Query: `SELECT [RealTimeDuplicateView].id as [Schema.id], [RealTime].id as [Dependency.id]
								FROM RealTimeDuplicateView
								JOIN RealTime on [RealTimeDuplicateView].id = [RealTime].id`,
							},
						},
					}
					var errs map[string]interface{}
					resp := configureRealTime("RealTimeDirectView", expectedSettings)
					unmarshallString(resp.Form.DataErrorsJson, &errs)
					Expect(errs).To(HaveKeyWithValue("tables", ContainElement(Not(HaveKey("query")))))
				})
			})

			It("should include tables as the enum for the table property", func() {
				resp := configureRealTime("RealTimeDirectView", RealTimeSettings{})
				jsonSchemaForForm := resp.GetJSONSchemaForForm()
				jsm := GetMapFromJSONSchema(jsonSchemaForForm)
				Expect(jsm).To(And(
					HaveKeyWithValue("properties",
						HaveKeyWithValue("tables",
							HaveKeyWithValue("items",
								HaveKeyWithValue("properties",
									HaveKeyWithValue("schemaID",
										And(
											HaveKeyWithValue("enum", And(
												ContainElement("[RealTime]"),
											),
											),
										),
									),
								),
							),
						),
					),
				))
			})

		})
	})

})
