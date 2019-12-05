package meta

import (
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"strings"
)

func ConvertSQLTypeToPluginType(t string, maxLength int) pub.PropertyType {
	text := strings.ToLower(strings.Split(t, "(")[0])

	switch text {
	case "datetime", "datetime2", "smalldatetime":
		return pub.PropertyType_DATETIME
	case "date":
		return pub.PropertyType_DATE
	case "time":
		return pub.PropertyType_TIME
	case "int", "smallint", "tinyint":
		return pub.PropertyType_INTEGER
	case "bigint", "decimal", "money", "smallmoney", "numeric":
		return pub.PropertyType_DECIMAL
	case "float", "real":
		return pub.PropertyType_FLOAT
	case "bit":
		return pub.PropertyType_BOOL
	case "binary", "varbinary", "image":
		return pub.PropertyType_BLOB
	case "char", "varchar", "nchar", "nvarchar", "text":
		if maxLength == -1 || maxLength >= 1024 {
			return pub.PropertyType_TEXT
		}
		return pub.PropertyType_STRING
	default:
		return pub.PropertyType_STRING
	}
}

func ConvertPluginTypeToSQLType(t pub.PropertyType) string {

	switch t {
	case pub.PropertyType_BOOL:
		return "bit"
	case pub.PropertyType_INTEGER:
		return "int"
	case pub.PropertyType_FLOAT:
		return "float"
	case pub.PropertyType_DECIMAL:
		return "decimal(38,18)"
	case pub.PropertyType_DATE:
		return "date"
	case pub.PropertyType_TIME:
		return "time"
	case pub.PropertyType_DATETIME:
		return "datetime"

	case pub.PropertyType_STRING,
		pub.PropertyType_TEXT,
		pub.PropertyType_BLOB,
		pub.PropertyType_JSON,
		pub.PropertyType_XML:
		return "varchar(max)"
	default:
		return "varchar(max)"
	}
}