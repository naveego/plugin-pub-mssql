package templates_test

import (
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	. "github.com/naveego/plugin-pub-mssql/internal/templates"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"log"
)

var _ = Describe("Templates", func() {

	It("should render self dependency", func() {
		actual, err := RenderSelfBridgeQuery(SelfBridgeQueryArgs{
			SchemaInfo: (&meta.Schema{
				ID: "test-schema",
			}).WithColumns([]*meta.Column{
				{ID: "[key1]", IsKey: true},
				{ID: "key2", IsKey: true},
			}),
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(ContainSubstring("[Schema.key1]"))
		Expect(actual).To(ContainSubstring("[Schema.key2]"))
	})

	DescribeTable("UniquifySQLID", func(input, expected string) {
		Expect(UniquifySQLName(input)).To(Equal(expected))
	},
		Entry("basic", "[table]", "[table_aab9]"),
		Entry("with schema", "[schema].[table]", "[schema_table_1a7b]"),

	)

	Describe("schema data query", func() {
		It("should render when there are filter columns", func() {
			const schemaID = "[RealTimeMergeView]"
			const depSchemaID = "[RealTime]"
			schema := &meta.Schema{
				ID:               schemaID,
				IsTable:          false,
				IsChangeTracking: false,
				Query:            "select [mergeValue], [count]\nfrom  [RealTimeMergeView]\n",
			}
			schema.AddColumn(
				&meta.Column{
					IsKey:        true,
					ID:           "[mergeValue]",
					SQLType:      "varchar(10)",
					PropertyType: pub.PropertyType_STRING,
					SchemaID:     schemaID,
				})
			schema.AddColumn(
				&meta.Column{
					ID:           "[count]",
					PropertyType: pub.PropertyType_INTEGER,
					SQLType:      "int",
					IsKey:        true,
					SchemaID:     schemaID,
				})

			depSchema := &meta.Schema{
				ID:               depSchemaID,
				IsTable:          true,
				IsChangeTracking: true,
				Query:            "SELECT [RealTime].mergeValue as [Schema.mergeValue], [RealTime].id as [Dependency.id] FROM RealTime",
			}

			depSchema.AddColumn(
				&meta.Column{
					IsKey:        true,
					ID:           "[id]",
					SQLType:      "int",
					PropertyType: pub.PropertyType_INTEGER,
				})
			depSchema.AddColumn(
				&meta.Column{
					ID:           "[ownValue]",
					PropertyType: pub.PropertyType_STRING,
					SQLType:      "varchar(10)",
				})
			depSchema.AddColumn(
				&meta.Column{
					ID:           "[mergeValue]",
					PropertyType: pub.PropertyType_STRING,
					SQLType:      "varchar(10)",
				})
			depSchema.AddColumn(
				&meta.Column{
					ID:           "[spreadValue]",
					PropertyType: pub.PropertyType_STRING,
					SQLType:      "varchar(10)",
				})

			args := SchemaDataQueryArgs{
				SchemaArgs:       schema,
				DependencyTables: []*meta.Schema{depSchema},
				RowKeys: []meta.RowKeys{
					{
						meta.RowKey{ColumnID: "[mergeValue]", Value: "updated"},
						meta.RowKey{ColumnID: "[count]", Value: 1},
					},
					{
						meta.RowKey{ColumnID: "[mergeValue]", Value: "inserted"},
						meta.RowKey{ColumnID: "[count]", Value: 2},
					},
				},
				DeletedFlagColumnName: "NAVEEGO_DELETED_COLUMN_FLAG",
			}

			result, err := RenderSchemaDataQuery(args)
			log.Println(result)
			Expect(err).ToNot(HaveOccurred())

		})
	})
})

// (*meta.Schema)(0xc000178000)({
// ID: (string) (len=19) "[RealTimeMergeView]",
// IsTable: (bool) false,
// IsChangeTracking: (bool) false,
// columns: (meta.Columns) (len=2 cap=2) {
// (*meta.Column)(0xc0007c85f0)([RealTimeMergeView].[count]),
// (*meta.Column)(0xc0007c85a0)([RealTimeMergeView].[mergeValue])
// },
// keyNames: ([]string) (len=1 cap=1) {
// (string) (len=12) "[mergeValue]"
// },
// keyColumns: ([]*meta.Column) (len=1 cap=1) {
// (*meta.Column)(0xc0007c85a0)([RealTimeMergeView].[mergeValue])
// },
// Query: (string) (len=55) "select [mergeValue], [count]\nfrom  [RealTimeMergeView]\n"
// }),
// DependencyTables: ([]*meta.Schema) (len=1 cap=1) {
// (*meta.Schema)(0xc0006263f0)({
// ID: (string) (len=10) "[RealTime]",
// IsTable: (bool) true,
// IsChangeTracking: (bool) true,
// columns: (meta.Columns) (len=4 cap=4) {
// (*meta.Column)(0xc0005d9040)([RealTime].[id]),
// (*meta.Column)(0xc0005d90e0)([RealTime].[mergeValue]),
// (*meta.Column)(0xc0005d9090)([RealTime].[ownValue]),
// (*meta.Column)(0xc0005d9130)([RealTime].[spreadValue])
// },
// keyNames: ([]string) (len=1 cap=1) {
// (string) (len=4) "[id]"
// },
// keyColumns: ([]*meta.Column) (len=1 cap=1) {
// (*meta.Column)(0xc0005d9040)([RealTime].[id])
// },
// Query: (string) (len=100) "SELECT [RealTime].mergeValue as [Schema.mergeValue], [RealTime].id as [Dependency.id] \nFROM RealTime"
// })
// },
// RowKeys: ([]meta.RowKeys) (len=2 cap=2) {
// (meta.RowKeys) (len=1 cap=1) [[mergeValue]:updated],
// (meta.RowKeys) (len=1 cap=4) [[mergeValue]:inserted]
// },
// DeletedFlagColumnName: (string) (len=27) "NAVEEGO_DELETED_COLUMN_FLAG"
// }
