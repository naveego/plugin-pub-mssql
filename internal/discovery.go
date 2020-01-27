package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	"github.com/pkg/errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// DiscoverSchemasSync discovers all the schemas synchronously, rather than streaming them.
func DiscoverSchemasSync(session *OpSession, schemaDiscoverer SchemaDiscoverer, req *pub.DiscoverSchemasRequest) ([]*pub.Schema, error) {
	discovered, err := schemaDiscoverer.DiscoverSchemas(session, req)
	if err != nil {
		return nil, err
	}

	ctx := session.Ctx

	var schemas []*pub.Schema

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case schema, more := <-discovered:
			if !more {
				sort.Sort(pub.SortableShapes(schemas))
				return schemas, nil
			}

			schemas = append(schemas, schema)
		}
	}

}

type SchemaDiscoverer struct {
	Log hclog.Logger
}




func (s *SchemaDiscoverer) DiscoverSchemas(session *OpSession, req *pub.DiscoverSchemasRequest) (<-chan *pub.Schema, error) {

	var err error
	var schemas []*pub.Schema

	if req.Mode == pub.DiscoverSchemasRequest_ALL {
		s.Log.Debug("Discovering all tables and views...")
		schemas, err = s.getAllSchemas(session)
		s.Log.Debug("Discovered tables and views.", "count", len(schemas))

		if err != nil {
			return nil, errors.Errorf("could not load tables and views from SQL: %s", err)
		}
	} else {
		s.Log.Debug("Refreshing schemas from request.", "count", len(req.ToRefresh))
		for _, s := range req.ToRefresh {
			schemas = append(schemas, s)
		}
	}

	resp := &pub.DiscoverSchemasResponse{}

	out := make(chan *pub.Schema)

	sort.Sort(pub.SortableShapes(resp.Schemas))

	go func() {
		wait := new(sync.WaitGroup)

		defer close(out)

		for i := range schemas {

			wait.Add(1)

			// concurrently get details for shape
			go func(schema *pub.Schema) {

				defer func() {
					out <- schema
					wait.Done()
				}()

				s.Log.Debug("Getting details for discovered schema", "id", schema.Id)
				err := s.populateShapeColumns(session, schema)
				if err != nil {
					s.Log.With("shape", schema.Id).With("err", err).Error("Error discovering columns")
					schema.Errors = append(schema.Errors, fmt.Sprintf("Could not discover columns: %s", err))
					return
				}

				s.Log.Debug("Got details for discovered schema", "id", schema.Id)

				if req.Mode == pub.DiscoverSchemasRequest_REFRESH {
					s.Log.Debug("Getting count for discovered schema", "id", schema.Id)
					schema.Count, err = s.getCount(session, schema)
					if err != nil {
						s.Log.With("shape", schema.Id).With("err", err).Error("Error getting row count.")
						schema.Errors = append(schema.Errors, fmt.Sprintf("Could not get row count for shape: %s", err))
						return
					}
					s.Log.Debug("Got count for discovered schema", "id", schema.Id, "count", schema.Count.String())
				} else {
					schema.Count = &pub.Count{Kind: pub.Count_UNAVAILABLE}
				}

				if req.SampleSize > 0 {
					s.Log.Debug("Getting sample for discovered schema", "id", schema.Id, "size", req.SampleSize)
					publishReq := &pub.ReadRequest{
						Schema: schema,
						Limit:  req.SampleSize,
					}

					collector := new(RecordCollector)
					handler, innerRequest, err := BuildHandlerAndRequest(session, publishReq, collector)
					if err != nil {
						s.Log.With("shape", schema.Id).With("err", err).Error("Error getting sample.")
						schema.Errors = append(schema.Errors, fmt.Sprintf("Could not get sample for shape: %s", err))
						return
					}

					err = handler.Handle(innerRequest)

					for _, record := range collector.Records {
						schema.Sample = append(schema.Sample, record)
					}

					if err != nil {
						s.Log.With("shape", schema.Id).With("err", err).Error("Error collecting sample.")
						schema.Errors = append(schema.Errors, fmt.Sprintf("Could not collect sample: %s", err))
						return
					}
					s.Log.Debug("Got sample for discovered schema", "id", schema.Id, "size", len(schema.Sample))
				}

			}(schemas[i])
		}

		// wait until all concurrent shape details have been loaded
		wait.Wait()

	}()

	return out, nil
}

func (s *SchemaDiscoverer) getAllSchemas(session *OpSession) ([]*pub.Schema, error) {

	rows, err := session.DB.Query(`SELECT Schema_name(o.schema_id) SchemaName, o.NAME TableName
FROM   sys.objects o 
WHERE  o.type IN ( 'U', 'V' )`)

	if err != nil {
		return nil, errors.Errorf("could not list tables: %s", err)
	}

	var schemas []*pub.Schema

	for rows.Next() {
		schema := new(pub.Schema)

		var (
			schemaName string
			tableName  string
		)
		err = rows.Scan(&schemaName, &tableName)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if schemaName == "dbo" {
			schema.Id = GetSchemaID(schemaName, tableName)
			schema.Name = tableName
		} else {
			schema.Id = GetSchemaID(schemaName, tableName)
			schema.Name = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		schema.DataFlowDirection = pub.Schema_READ

		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func GetSchemaInfo(db *sql.DB) (map[string]*meta.Schema, error) {
	schemaInfo := map[string]*meta.Schema{}

	rows, err := db.Query(`SELECT t.TABLE_NAME
     , t.TABLE_SCHEMA
     , t.TABLE_TYPE
     , c.COLUMN_NAME
     , tc.CONSTRAINT_TYPE
, CASE
  WHEN exists (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME))
  THEN 1
  ELSE 0
  END AS CHANGE_TRACKING
FROM INFORMATION_SCHEMA.TABLES AS t
       INNER JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
       LEFT OUTER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
                       ON ccu.COLUMN_NAME = c.COLUMN_NAME AND ccu.TABLE_NAME = t.TABLE_NAME AND
                          ccu.TABLE_SCHEMA = t.TABLE_SCHEMA
       LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                       ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME AND tc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA

ORDER BY TABLE_NAME`)

	if err != nil {
		return nil, errors.Errorf("could not read database schema: %s", err)
	}

	// Collect table names for display in UIs.
	for rows.Next() {
		var (
			schema, table, typ, columnName string
			constraint                     *string
			changeTracking                 bool
		)
		err = rows.Scan(&table, &schema, &typ, &columnName, &constraint, &changeTracking)
		if err != nil {
			return nil, errors.Wrap(err, "could not read table schema")
		}
		id := GetSchemaID(schema, table)
		info, ok := schemaInfo[id]
		if !ok {
			info = &meta.Schema{
				ID:               id,
				IsTable:          typ == "BASE TABLE",
				IsChangeTracking: changeTracking,
			}
			schemaInfo[id] = info
		}
		columnName = fmt.Sprintf("[%s]", columnName)
		columnInfo, ok := info.LookupColumn(columnName)
		if !ok {
			columnInfo = info.AddColumn(&meta.Column{ID: columnName})
		}
		columnInfo.IsKey = columnInfo.IsKey || constraint != nil && *constraint == "PRIMARY KEY"
	}

	return schemaInfo, nil
}

func GetSchemaID(schemaName, tableName string) string {
	if schemaName == "dbo" {
		return fmt.Sprintf("[%s]", tableName)
	} else {
		return fmt.Sprintf("[%s].[%s]", schemaName, tableName)
	}
}

type describeResult struct {
	IsHidden          bool   `sql:"is_hidden"`
	Name              string `sql:"name"`
	SystemTypeName    string `sql:"system_type_name"`
	IsNullable        bool   `sql:"is_nullable"`
	IsPartOfUniqueKey bool   `sql:"is_part_of_unique_key"`
	MaxLength         int64  `sql:"max_length"`
}

func describeResultSet(session *OpSession, query string) ([]describeResult, error) {
	//metaQuery := " @query, @params= N'', @browse_information_mode=1"

	rows, err := session.DB.Query("sp_describe_first_result_set",
		sql.Named("query", query),
		sql.Named("params",""),
		sql.Named("browse_information_mode",1),
	)

	if err != nil {

		rows, betterErr := session.DB.Query(query)
		if betterErr == nil {
			rows.Close()
			return nil, errors.Errorf("unhelpful error returned by MSSQL when getting metadata for query %q: %s", query, err)
		} else {
			return nil, errors.Errorf("error when getting metadata for query %q: %s", query, betterErr)
		}
	}

	metadata := make([]describeResult, 0, 0)

	defer rows.Close()
	err = sqlstructs.UnmarshalRows(rows, &metadata)
	if err != nil {
		return nil, errors.Errorf("error parsing metadata for query %q: %s", query, err)
	}

	return metadata, nil
}

func (s *SchemaDiscoverer) populateShapeColumns(session *OpSession, shape *pub.Schema) error {

	query := shape.Query
	if query == "" {
		query = fmt.Sprintf("SELECT * FROM %s", shape.Id)
	}

	query = strings.Replace(query, "'", "''", -1)

	metadata, err := describeResultSet(session, query)
	if err != nil {
		return err
	}

	unnamedColumnIndex := 0

	preDefinedProperties := map[string]*pub.Property{}
	hasUserDefinedKeys := false
	for _, p := range shape.Properties {
		preDefinedProperties[p.Id] = p
		if p.IsKey {
			hasUserDefinedKeys = true
		}
	}

	var discoveredProperties []*pub.Property

	for _, m := range metadata {

		if m.IsHidden {
			continue
		}

		var property *pub.Property
		var ok bool
		var propertyID string

		propertyName := m.Name
		if propertyName == "" {
			propertyName = fmt.Sprintf("UNKNOWN_%d", unnamedColumnIndex)
			unnamedColumnIndex++
		}

		propertyID = fmt.Sprintf("[%s]", propertyName)

		if property, ok = preDefinedProperties[propertyID]; !ok {
			property = &pub.Property{
				Id:   propertyID,
				Name: propertyName,
			}
		}

		discoveredProperties = append(discoveredProperties, property)

		property.TypeAtSource = m.SystemTypeName

		maxLength := m.MaxLength
		property.Type = meta.ConvertSQLTypeToPluginType(property.TypeAtSource, int(maxLength))

		property.IsNullable = m.IsNullable

		if !hasUserDefinedKeys {
			property.IsKey = m.IsPartOfUniqueKey
		}
	}

	shape.Properties = discoveredProperties

	return nil
}

func (s *SchemaDiscoverer) getCount(session *OpSession, shape *pub.Schema) (*pub.Count, error) {

	var query string
	var err error

	schemaInfo := session.SchemaInfo[shape.Id]

	if shape.Query != "" {
		query = fmt.Sprintf("SELECT COUNT(1) FROM (%s) as Q", shape.Query)
	} else if schemaInfo == nil || !schemaInfo.IsTable {
		return &pub.Count{Kind: pub.Count_UNAVAILABLE}, nil
	} else {
		segs := schemaIDParseRE.FindStringSubmatch(shape.Id)
		if segs == nil {
			return nil, fmt.Errorf("malformed schema ID %q", shape.Id)
		}

		schema, table := segs[1], segs[2]
		if schema == "" {
			schema = "dbo"
		}

		query = fmt.Sprintf(`
			SELECT SUM(p.rows) FROM sys.partitions AS p
			INNER JOIN sys.tables AS t
			ON p.[object_id] = t.[object_id]
			INNER JOIN sys.schemas AS s
			ON s.[schema_id] = t.[schema_id]
			WHERE t.name = N'%s'
			AND s.name = N'%s'
			AND p.index_id IN (0,1);`, table, schema)
	}

	ctx, cancel := context.WithTimeout(session.Ctx, time.Second)
	defer cancel()
	row := session.DB.QueryRowContext(ctx, query)
	var count int
	err = row.Scan(&count)
	if err != nil {
		if strings.Contains(err.Error(), "context deadline exceeded") {
			return &pub.Count{
				Kind:  pub.Count_UNAVAILABLE,
			}, nil
		}
		return nil, fmt.Errorf("error from query %q: %s", query, err)
	}

	return &pub.Count{
		Kind:  pub.Count_EXACT,
		Value: int32(count),
	}, nil
}
