package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/api/utils"
	"github.com/naveego/navigator-go/publishers/protocol"
	"github.com/sirupsen/logrus"
)

type mssqlClient struct {
	mu               *sync.Mutex
	connectionString string
	publishing       bool
	stopPublish      func()
}

// NewmssqlClient creates a new MSSQL publisher instance
func NewClient() *mssqlClient {
	return &mssqlClient{
		mu: &sync.Mutex{},
	}
}

func (m *mssqlClient) Init(request protocol.InitRequest) (protocol.InitResponse, error) {
	e := func(msg string) (protocol.InitResponse, error) {
		return protocol.InitResponse{Message: msg}, errors.New(msg)
	}

	if m.publishing {
		return e("A publish is still running. You must wait for it to finish or call Dispose to cancel it.")
	}

	// TestConnection sets m.connectionString
	tr, err := m.TestConnection(protocol.TestConnectionRequest{Settings: request.Settings})

	return protocol.InitResponse{
		Success: true,
		Message: tr.Message,
	}, err
}

func (m *mssqlClient) Dispose(protocol.DisposeRequest) (protocol.DisposeResponse, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.publishing && m.stopPublish != nil {
		m.stopPublish()
		return protocol.DisposeResponse{
			Success: true,
		}, nil
	}

	return protocol.DisposeResponse{
		Success: false,
		Message: "No publish running.",
	}, nil
}

func (m *mssqlClient) Publish(request protocol.PublishRequest, toClient protocol.PublisherClient) (protocol.PublishResponse, error) {
	logrus.WithField("publish-request", request).Debug("Publish")

	e := func(msg string) (protocol.PublishResponse, error) {
		return protocol.PublishResponse{Message: msg}, errors.New(msg)
	}

	if m.connectionString == "" {
		return e("You must call Init before publishing.")
	}

	conn, err := sql.Open("mssql", m.connectionString)
	if err != nil {
		return e("Could not open connection to publisher: " + err.Error())
	}

	shapes, err := discoverShapes(conn)
	if err != nil {
		return e("Could not discover shapes: " + err.Error())
	}

	var (
		shapeDef   pipeline.ShapeDefinition
		shape      pipeline.Shape
		foundShape bool
	)

	for _, s := range shapes {
		if s.Name == request.ShapeName {
			shapeDef = s
			foundShape = true
		}
	}
	if !foundShape {
		return e(fmt.Sprintf("No shape discovered with requested ID '%s'.", request.ShapeName))
	}

	columns := []string{}

	shape.KeyNames = shapeDef.Keys

	for _, v := range shapeDef.Properties {
		columns = append(columns, v.Name)
		shape.Properties = append(shape.Properties, v.Name+":"+v.Type)
	}

	var safeTableName string

	if strings.Contains(shapeDef.Name, ".") {
		nameParts := strings.Split(shapeDef.Name, ".")
		safeTableName = "[" + strings.Join(nameParts, "],[") + "]"
	}

	colStr := "[" + strings.Join(columns, "],[") + "]"
	query := fmt.Sprintf("SELECT %s FROM %s", colStr, safeTableName)

	logrus.Debugf("Query: %s", query)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.publishing {
		return e("A publish is still running. You must wait for it to finish or call Dispose to cancel it.")
	}

	ctx, cancel := context.WithCancel(context.Background())

	m.stopPublish = func() {
		cancel()
		m.publishing = false
	}
	m.publishing = true

	go func() {
		defer func() {
			toClient.Done(protocol.DoneRequest{})
			m.publishing = false
		}()

		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			logrus.WithError(err).Warn("Could not query publisher")
			return
		}
		defer rows.Close()

		vals := make([]interface{}, len(columns))
		for i := 0; i < len(columns); i++ {
			vals[i] = new(interface{})
		}

		for rows.Next() {

			if ctx.Err() != nil {
				break
			}

			err := rows.Scan(vals...)
			if err != nil {
				logrus.WithError(err).Warn("Error reading row")
				continue
			}

			d := map[string]interface{}{}

			for i := 0; i < len(vals); i++ {
				colName := columns[i]
				d[colName] = vals[i]
			}

			dp := pipeline.DataPoint{
				Entity:   shapeDef.Name,
				Data:     d,
				KeyNames: shape.KeyNames,
				Shape:    shape,
			}

			dps := []pipeline.DataPoint{dp}
			toClient.SendDataPoints(protocol.SendDataPointsRequest{DataPoints: dps})
		}
	}()

	return protocol.PublishResponse{
		Success: true,
		Message: "Publish started. Call Dispose to stop.",
	}, nil

}

func (m *mssqlClient) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {

	response := protocol.DiscoverShapesResponse{}
	var err error
	if m.connectionString == "" {
		m.connectionString, err = buildConnectionString(request.Settings, 30)
		if err != nil {
			return response, err
		}
	}

	conn, err := sql.Open("mssql", m.connectionString)
	if err != nil {
		return response, err
	}
	defer conn.Close()

	shapes, err := discoverShapes(conn)

	return protocol.DiscoverShapesResponse{
		Shapes: shapes,
	}, err

}

func discoverShapes(conn *sql.DB) ([]pipeline.ShapeDefinition, error) {

	var (
		shapes []pipeline.ShapeDefinition
	)

	rows, err := conn.Query(`SELECT Schema_name(o.schema_id) SchemaName, 
	o.NAME                   TableName, 
	col.NAME                 ColumnName, 
	ty.NAME                  TypeName, 
	CASE 
	  WHEN EXISTS (SELECT 1 
				   FROM   sys.indexes ind 
						  INNER JOIN sys.index_columns indcol 
								  ON indcol.object_id = o.object_id 
									 AND indcol.index_id = ind.index_id 
									 AND col.column_id = indcol.column_id 
				   WHERE  ind.object_id = o.object_id 
						  AND ind.is_primary_key = 1) THEN 1 
	  ELSE 0 
	END                      AS IsPrimaryKey 
FROM   sys.objects o 
	INNER JOIN sys.columns col 
			ON col.object_id = o.object_id 
	INNER JOIN sys.types ty 
			ON ( col.user_type_id = ty.user_type_id ) 
WHERE  o.type IN ( 'U', 'V' ) 
ORDER  BY Schema_name(o.schema_id), 
	   o.NAME, 
	   col.column_id`)

	if err != nil {
		return shapes, err
	}
	defer rows.Close()

	var schemaName string
	var tableName string
	var columnName string
	var columnType string
	var isPrimaryKey bool

	s := map[string]*pipeline.ShapeDefinition{}

	for rows.Next() {
		err = rows.Scan(&schemaName, &tableName, &columnName, &columnType, &isPrimaryKey)
		if err != nil {
			return nil, err
		}

		shapeName := tableName
		if schemaName != "dbo" {
			shapeName = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		shapeDef, ok := s[shapeName]
		if !ok {
			shapeDef = &pipeline.ShapeDefinition{
				Name: shapeName,
			}
			s[shapeName] = shapeDef
		}

		shapeDef.Properties = append(shapeDef.Properties, pipeline.PropertyDefinition{
			Name: columnName,
			Type: convertSQLType(columnType),
		})

		if isPrimaryKey {
			shapeDef.Keys = append(shapeDef.Keys, columnName)
		}
	}

	for _, sd := range s {
		shapes = append(shapes, *sd)
	}

	// Sort the shapes by Name
	sort.Sort(pipeline.SortShapesByName(shapes))

	return shapes, nil
}

func (m *mssqlClient) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {
	r := func(ok bool, msg string) protocol.TestConnectionResponse {
		return protocol.TestConnectionResponse{Success: ok, Message: msg}
	}
	var err error
	m.connectionString, err = buildConnectionString(request.Settings, 10)
	if err != nil {
		return r(false, "could not connect to server"), err
	}
	conn, err := sql.Open("mssql", m.connectionString)
	if err != nil {
		return r(false, "could not connect to server"), err
	}
	defer conn.Close()

	stmt, err := conn.Prepare("select 1")
	if err != nil {
		return r(false, "could not connect to server"), err
	}
	defer stmt.Close()

	return r(true, "successfully connected to server"), nil
}

func buildConnectionString(settings map[string]interface{}, timeout int) (string, error) {

	mr := utils.NewMapReader(settings)
	server, ok := mr.ReadString("server")
	if !ok {
		return "", errors.New("server cannot be null or empty")
	}
	db, ok := mr.ReadString("database")
	if !ok {
		return "", errors.New("database cannot be null or empty")
	}
	auth, ok := mr.ReadString("auth")
	if !ok {
		return "", errors.New("auth type must be provided")
	}

	connStr := []string{
		"server=" + server,
		"database=" + db,
		"connection timeout=10",
	}

	if auth == "sql" {
		username, _ := mr.ReadString("username")
		pwd, _ := mr.ReadString("password")
		connStr = append(connStr, "user id="+username)
		connStr = append(connStr, "password="+pwd)
	} else {
		connStr = append(connStr, "")
	}

	conn := strings.Join(connStr, ";")

	logrus.Debugf("connection string: %s", conn)

	return conn, nil
}

func convertSQLType(t string) string {
	text := strings.ToLower(strings.Split(t, "(")[0])

	switch text {
	case "datetime", "datetime2", "date", "time", "smalldatetime":
		return "date"
	case "bigint", "int", "smallint", "tinyint":
		return "integer"
	case "decimal", "float", "money", "smallmoney":
		return "float"
	case "bit":
		return "bool"
	}
	return "string"
}
