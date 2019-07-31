package internal

import (
	"crypto/md5"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)

func MetaSchemaFromPubSchema(sourceSchema *pub.Schema) *meta.Schema {
	schema := &meta.Schema{
		ID:    sourceSchema.Id,
		Query: sourceSchema.Query,
	}

	if schema.ID == "" {
		// User defined queries may not have an assigned ID,
		// so we'll generate one based on the query.
		// This has the nice property that if the query changes
		// we'll treat it as a new schema, which it effectively is.
		h := md5.New()
		h.Write([]byte(schema.Query))
		o := h.Sum(nil)
		schema.ID = fmt.Sprintf("udq_%x", o)
	}

	for _, p := range sourceSchema.Properties {
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