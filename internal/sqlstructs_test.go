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

		query := "select top (1) * from Types"
		actualRows, err := db.Query(query)
		Expect(err).ToNot(HaveOccurred())

		actual, err := sqlstructs.UnmarshalRowsToMaps(actualRows)
		Expect(err).ToNot(HaveOccurred())

		Expect(actual).To(HaveLen(1))

		// spew.Dump(actual)

		Expect(actual).To(ContainElement(
			And(
		HaveKeyWithValue("int",int64(42) ),
		HaveKeyWithValue("bigint",			int64(9223372036854775807)),
		HaveKeyWithValue("numeric", "1234.56780"),
		HaveKeyWithValue("bit", true),
		HaveKeyWithValue("smallint", int64(123)),
		HaveKeyWithValue("decimal", "1234.5678"),
		HaveKeyWithValue("smallmoney", "12.5600"),
		HaveKeyWithValue("tinyint", int64(12)),
		HaveKeyWithValue("money", "1234.5600"),
		HaveKeyWithValue("float", float64(123456.789)),
		HaveKeyWithValue("real", float64(123456.7890625)),
		HaveKeyWithValue("char", "char  "),
		HaveKeyWithValue("varchar", "abc"),
		HaveKeyWithValue("text", "abc"),
		HaveKeyWithValue("nchar", "nchar "),
		HaveKeyWithValue("nvarchar", "nvarchar"),
		HaveKeyWithValue("ntext", "ntext"),
		HaveKeyWithValue("uniqueidentifier", "ff0df4a2-c2b6-11ea-93a8-e335bc66abae"),
				)))
	})
})
