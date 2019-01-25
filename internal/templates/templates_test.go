package templates_test

import (
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	. "github.com/naveego/plugin-pub-mssql/internal/templates"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Templates", func() {

	It("should render self dependency", func(){
		actual, err := RenderSelfBridgeQuery(SelfBridgeQueryArgs{
			SchemaInfo:(&meta.Schema{
				ID: "test-schema",
			}).WithColumns([]*meta.Column{
					{ID: "[key1]", IsKey:true},
					{ID: "key2", IsKey:true},
				}),
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(ContainSubstring("[Schema.key1]"))
		Expect(actual).To(ContainSubstring("[Schema.key2]"))
	})
})
