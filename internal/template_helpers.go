package internal

import (
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)

func MetaSchemaFromShape(shape *pub.Schema) *meta.Schema {
	schema := &meta.Schema{
		ID:shape.Id,
		Query:shape.Query,
	}

	for _, p := range shape.Properties {
		col := &meta.Column{
			ID:           p.Id,
			IsKey:        p.IsKey,
			PropertyType: p.Type,
			SQLType: p.TypeAtSource,
		}

		schema.AddColumn(col)
	}
	return schema
}