package internal_test

import (
	. "github.com/naveego/plugin-pub-mssql/internal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Settings", func() {

	var (
		settings Settings
	)

	BeforeEach(func() {
		settings = Settings{
			Host:     "localhost:1433",
			Database: "w3",
			Auth:     AuthTypeSQL,
			Username: "sa",
			Password: "n5o_ADMIN",
		}
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
