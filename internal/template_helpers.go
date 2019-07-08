package internal

import (
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)

func MetaSchemaFromPubSchema(shape *pub.Schema) *meta.Schema {
	schema := &meta.Schema{
		ID:shape.Id,
		Query:shape.Query,
	}

	for _, p := range shape.Properties {
		col := &meta.Column{
			ID:           p.Id,
			IsKey:        p.IsKey,
			PropertyType: p.Type,
		}
		if p.TypeAtSource == "" {
			col.SQLType = meta.ConvertPluginTypeToSQLType(p.Type)
		} else {
			col.SQLType = p.TypeAtSource
		}

		schema.AddColumn(col)
	}
	return schema
}