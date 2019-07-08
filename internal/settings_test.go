package internal_test

import (
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Settings", func() {

	var (
		settings Settings
	)

	BeforeEach(func() {
		settings = *GetTestSettings()
	})


	Describe("Validate", func() {

		It("Should error if host is not set", func() {
			settings.Host = ""
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

		It("Should error if auth is unknown", func() {
			settings.Auth = AuthType("bogus")
			settings.Username = ""
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

		It("Should succeed if settings are valid for sql", func() {
			Expect(settings.Validate()).To(Succeed())
		})

		It("Should succeed if settings are valid for windows", func() {
			settings.Auth = AuthTypeWindows
			settings.Username = ""
			settings.Password = ""
			Expect(settings.Validate()).To(Succeed())
		})
	})
})

var _ = Describe("UnmarshalledRecord", func(){
	It("should have correct data", func(){
		input := &pub.Record{
			DataJson:`{"a":"A","b":2}`,
			Versions: []*pub.RecordVersion{
				{
					DataJson:`{"a":"vA","b":3}`,
				},{
					DataJson:`{"a":"vB","b":4}`,
				},
			},
		}
		actual, err := input.AsUnmarshalled()
		Expect(err).ToNot(HaveOccurred())

		Expect(actual.Data).To(BeEquivalentTo(map[string]interface{}{
			"a":"A",
			"b":float64(2),
		}))
		Expect(actual.UnmarshalledVersions).To(HaveLen(2))
		Expect(actual.UnmarshalledVersions[0].Data).To(BeEquivalentTo(
			map[string]interface{}{
			"a":"vA",
			"b":float64(3),
		}))
		Expect(actual.UnmarshalledVersions[1].Data).To(BeEquivalentTo(
			map[string]interface{}{
			"a":"vB",
			"b":float64(4),
		}))
	})
})