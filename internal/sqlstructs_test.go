package internal

import (
	"database/sql"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/url"
)

type AgentEntity struct {
	AgentCode string `sql:"AGENT_CODE"`
	COMMISSION float64
}

var _ = Describe("sqlstructs", func() {
	It("should populate slice", func() {
		u := &url.URL{
			Scheme:   "sqlserver",
			Host:     "localhost:1433",
			RawQuery: "database=w3",
		}
		u.User = url.UserPassword("sa", "n5o_ADMIN")

		db, err := sql.Open("sqlserver", u.String())
		Expect(err).ToNot(HaveOccurred())

		query := "select top (5) * from Agents order by AGENT_CODE"
		actualRows, err := db.Query(query)
		Expect(err).ToNot(HaveOccurred())

		actual := make([]AgentEntity, 0, 0)

		Expect(sqlstructs.UnmarshalRows(actualRows, &actual)).To(Succeed())

		Expect(actual).To(HaveLen(5))

		Expect(actual).To(ContainElement(AgentEntity{"A001",0.14}))
		Expect(actual).To(ContainElement(AgentEntity{"A002",0.11}))
		fmt.Println(actual)
	})
})
