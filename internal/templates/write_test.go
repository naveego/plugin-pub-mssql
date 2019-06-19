package templates_test

import (
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"

	. "github.com/naveego/plugin-pub-mssql/internal/templates"
)

var _ = Describe("RenderReplicationTableCreationDDLArgs", func() {

	It("should render correctly", func(){
		actual, err := RenderReplicationTableCreationDDLArgs(ReplicationTableCreationDDL{
			Schema:(&meta.Schema{
				ID: "[test-schema].[test-table]",
			}).WithColumns([]*meta.Column{
				{ID: "[key1]", IsKey:true, SQLType:"int"},
				{ID: "key2", IsKey:true, SQLType: "char(5)"},
				{ID: "value1", SQLType: "varchar(max)"},
			}),
		})
		_, _ = fmt.Fprintln(os.Stdout, actual)
		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(ContainSubstring("[key1] int,"))
		Expect(actual).To(ContainSubstring("key2 char(5),"))
		Expect(actual).To(ContainSubstring("value1 varchar(max),"))
		Expect(actual).To(ContainSubstring("PRIMARY KEY ([key1], key2)"))
	})
})
