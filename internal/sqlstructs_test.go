package internal_test

import (
	"database/sql"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type AgentEntity struct {
	AgentCode  string `sql:"AGENT_CODE"`
	COMMISSION float64
}

var _ = Describe("sqlstructs", func() {
	It("should populate slice", func() {
		var err error
		var connectionString string

		connectionString, err = GetTestSettings().GetConnectionString()
		Expect(err).ToNot(HaveOccurred())

		db, err := sql.Open("sqlserver", connectionString)
		Expect(err).ToNot(HaveOccurred())

		query := "select top (5) * from Agents order by AGENT_CODE"
		actualRows, err := db.Query(query)
		Expect(err).ToNot(HaveOccurred())

		actual := make([]AgentEntity, 0, 0)

		Expect(sqlstructs.UnmarshalRows(actualRows, &actual)).To(Succeed())

		Expect(actual).To(HaveLen(5))

		Expect(actual).To(ContainElement(AgentEntity{"A001", 0.14}))
		Expect(actual).To(ContainElement(AgentEntity{"A002", 0.11}))
	})

	It("should populate slice of maps", func() {
		var err error
		var connectionString string

		connectionString, err = GetTestSettings().GetConnectionString()
		Expect(err).ToNot(HaveOccurred())

		db, err := sql.Open("sqlserver", connectionString)
		Expect(err).ToNot(HaveOccurred())

		query := "select top (5) * from Agents order by AGENT_CODE"
		actualRows, err := db.Query(query)
		Expect(err).ToNot(HaveOccurred())

		actual, err := sqlstructs.UnmarshalRowsToMaps(actualRows)
		Expect(err).ToNot(HaveOccurred())

		Expect(actual).To(HaveLen(5))

		Expect(actual).To(ContainElement(
			And(
				HaveKeyWithValue("AGENT_CODE","A001"),
				HaveKeyWithValue("COMMISSION",0.14),
				)))
	})
})
