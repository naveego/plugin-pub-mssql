package internal_test

import (
	"context"
	"encoding/json"
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

		var configureRealTime = func(schema *pub.Schema, settings RealTimeSettings) *pub.ConfigureRealTimeResponse {
			shape := discoverShape(sut, schema)
			req := (&pub.ConfigureRealTimeRequest{
				Schema: shape,
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
				It("should return a form schema with only the polling property", func() {
					resp := configureRealTime(&pub.Schema{Id: "[RealTime]"}, RealTimeSettings{})
					jsonSchemaForForm := resp.GetJSONSchemaForForm()
					Expect(jsonSchemaForForm.Properties).To(HaveLen(1))
					Expect(jsonSchemaForForm.Description).ToNot(BeEmpty())
				})
			})
		})

		Describe("when schema is a view", func() {

			It("should have error if table does not have change tracking enabled", func() {

				resp := configureRealTime(&pub.Schema{Id: "[RealTimeDuplicateView]"}, RealTimeSettings{
					Tables: []RealTimeTableSettings{
						{SchemaID: "[Customers]"},
					},
				})
				var errorMap ErrorMap
				Expect(json.Unmarshal([]byte(resp.Form.DataErrorsJson), &errorMap)).To(Succeed())
				errs := errorMap.GetErrors("tables", "0", "schemaID")
				Expect(errs).To(
					ContainElement(
						ContainSubstring("Table does not have change tracking enabled.")))
				})

			It("should round trip settings", func() {
				var actual RealTimeSettings
				expectedSettings := RealTimeSettings{
					Tables: []RealTimeTableSettings{
						{SchemaID: "[RealTime]"},
					},
				}
				resp := configureRealTime(&pub.Schema{Id: "[RealTimeDuplicateView]"}, expectedSettings)
				unmarshallString(resp.Form.DataJson, &actual)
				Expect(actual).To(BeEquivalentTo(expectedSettings))
			})

			Describe("query validation", func() {

				It("should detect invalid query", func() {
					expectedSettings := RealTimeSettings{
						Tables: []RealTimeTableSettings{
							{
								SchemaID: "[RealTime]",
								Query: `SELECT [RealTimeDuplicateView].recordID as [Source.recordID], [RealTime].id as [Dep.id]
								FROM RealTimeDuplicateView
								JOIN RealTime on [RealTimeDuplicateView].recordID = [RealTime].id`,
							},
						},
					}
					resp := configureRealTime(&pub.Schema{
						Id: "[RealTimeDuplicateView]",
						Properties:[]*pub.Property{
							{Name:"recordID", Id:"[recordID]", IsKey:true, Type:pub.PropertyType_INTEGER, TypeAtSource:"int"},
							{Name:"ownValue", Id:"[ownValue]", Type:pub.PropertyType_STRING, TypeAtSource:"varchar(10)"},
							{Name:"mergeValue", Id:"[mergeValue]", Type:pub.PropertyType_STRING, TypeAtSource:"varchar(10)"},
							{Name:"spreadValue", Id:"[spreadValue]", Type:pub.PropertyType_STRING, TypeAtSource:"varchar(10)"},
						},

					}, expectedSettings)
					var errorMap ErrorMap
					unmarshallString(resp.Form.DataErrorsJson, &errorMap)
					errs := errorMap.GetErrors("tables", "0", "query")
					Expect(errs).To(ContainElement(
						And(
							ContainSubstring("[Schema.recordID]"),
							ContainSubstring("[Dependency.id]"),
						)))
				})

				It("should detect valid query", func() {
					expectedSettings := RealTimeSettings{
						Tables: []RealTimeTableSettings{
							{
								SchemaID: "[RealTime]",
								Query: `SELECT [RealTimeDuplicateView].recordID as [Schema.recordID], [RealTime].id as [Dependency.id]
								FROM RealTimeDuplicateView
								JOIN RealTime on [RealTimeDuplicateView].recordID = [RealTime].id and 'a' = 'a'`,
							},
						},
					}
					var errorMap ErrorMap
					resp := configureRealTime(&pub.Schema{Id: "[RealTimeDuplicateView]"}, expectedSettings)
					unmarshallString(resp.Form.DataErrorsJson, &errorMap)
					errs := errorMap.GetErrors("tables", "0", "query")
					Expect(errs).To(BeEmpty())
				})
			})

			It("should include tables as the enum for the table property", func() {
				resp := configureRealTime(&pub.Schema{Id: "[RealTimeDuplicateView]"}, RealTimeSettings{})
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
