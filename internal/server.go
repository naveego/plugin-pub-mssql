package internal

import (
	"os"
	"sync"

	"context"

	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/hashicorp/go-hclog"
	"encoding/json"
	"github.com/pkg/errors"
	"fmt"
	"database/sql"
	"net/url"
	"strings"
	"sort"
)

type Server struct {
	mu         *sync.Mutex
	log        hclog.Logger
	settings   *Settings
	db         *sql.DB
	publishing bool
	connected  bool
}

// NewServer creates a new publisher Server.
func NewServer() pub.PublisherServer {
	return &Server{
		mu: &sync.Mutex{},
		log: hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Trace,
			Output:     os.Stderr,
			JSONFormat: true,
		}),
	}
}

func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {
	s.settings = nil
	s.connected = false

	settings := new(Settings)
	if err := json.Unmarshal([]byte(req.SettingsJson), settings); err != nil {
		return nil, err
	}

	if err := settings.Validate(); err != nil {
		return nil, err
	}

	u := &url.URL{
		Scheme: "sqlserver",
		Host:   settings.Server,
		// Path:  instance, // if connecting to an instance instead of a port
		RawQuery: fmt.Sprintf("database=%s", settings.Database),
	}
	switch settings.Auth {
	case AuthTypeSQL:
		u.User = url.UserPassword(settings.Username, settings.Password)
	}

	var err error
	s.db, err = sql.Open("sqlserver", u.String())
	if err != nil {
		return nil, fmt.Errorf("could not open connection: %s", err)
	}

	_, err = s.db.Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ")

	if err != nil {
		return nil, fmt.Errorf("could not read database schema: %s", err)
	}

	// connection made and tested

	s.connected = true
	s.settings = settings

	return new(pub.ConnectResponse), err
}

func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverShapesRequest) (*pub.DiscoverShapesResponse, error) {

	if !s.connected {
		return nil, errNotConnected
	}

	rows, err := s.db.Query(`SELECT Schema_name(o.schema_id) SchemaName, 
	o.NAME                   TableName, 
	col.NAME                 ColumnName, 
	ty.NAME                  TypeName, 
    col.max_length           MaxLength,
  	col.precision            [Precision],
  	col.scale                [Scale],
	col.is_nullable          nullable,
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
			ON ( col.system_type_id = ty.system_type_id ) 
WHERE  o.type IN ( 'U', 'V' ) 
ORDER  BY Schema_name(o.schema_id), 
	   o.NAME, 
	   col.column_id`)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var schemaName string
	var tableName string
	var columnName string
	var columnType string
	var maxLength int
	var precision int
	var scale int
	var isNullable bool
	var isPrimaryKey bool

	shapes := map[string]*pub.Shape{}

	for _, shape := range req.ToRefresh {
		shapes[shape.Id] = shape
		shape.Properties = nil
	}

	for rows.Next() {
		err = rows.Scan(&schemaName, &tableName, &columnName, &columnType, &maxLength, &precision, &scale, &isNullable, &isPrimaryKey)
		if err != nil {
			return nil, err
		}

		var (
			shapeID   string
			shapeName string
		)
		if schemaName == "dbo" {
			shapeID = fmt.Sprintf("[%s]", tableName)
			shapeName = tableName
		} else {
			shapeID = fmt.Sprintf("[%s].[%s]", schemaName, tableName)
			shapeName = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		// in refresh mode, only refresh requested shapes
		if req.Mode == pub.DiscoverShapesRequest_REFRESH {
			if _, ok := shapes[shapeID]; !ok {
				continue
			}
		}

		shape, ok := shapes[shapeID]
		if !ok {
			shape = &pub.Shape{
				Id:   shapeID,
				Name: shapeName,
			}
			shapes[shapeID] = shape
		}

		property := &pub.Property{
			Id:           fmt.Sprintf("[%s]", columnName),
			Name:         columnName,
			Type:         convertSQLType(columnType, maxLength),
			TypeAtSource: formatTypeAtSource(columnType, maxLength, precision, scale),
			IsKey:        isPrimaryKey,
			IsNullable:   isNullable,
		}

		shape.Properties = append(shape.Properties, property)
	}

	resp := &pub.DiscoverShapesResponse{}

	for _, shape := range shapes {
		sort.Sort(pub.SortableProperties(shape.Properties))

		shape.Count, err = s.getCount(shape)
		if err != nil {
			return nil, fmt.Errorf("could not get row count for shape %q: %s", shape.Id, err)
		}

		if req.SampleSize > 0 {
			publishReq := &pub.PublishRequest{
				Shape: shape,
				Limit: req.SampleSize,
			}
			records := make(chan *pub.Record)

			go func() {
				err = s.readRecords(ctx, publishReq, records)
			}()

			for record := range records {
				shape.Sample = append(shape.Sample, record)
			}

			if err != nil {
				return nil, fmt.Errorf("error while collecting sample: %s", err)
			}
		}

		resp.Shapes = append(resp.Shapes, shape)
	}

	sort.Sort(pub.SortableShapes(resp.Shapes))

	return resp, nil

}

func (s *Server) getCount(shape *pub.Shape) (*pub.Count, error) {

	var count int

	row := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s", shape.Id))

	err := row.Scan(&count)
	if err != nil {
		return nil, err
	}

	return &pub.Count{
		Kind:  pub.Count_EXACT,
		Value: int32(count),
	}, nil
}

func (s *Server) readRecords(ctx context.Context, req *pub.PublishRequest, out chan<- *pub.Record) error {

	defer close(out)

	var err error

	query := req.Shape.Query
	if req.Shape.Query == "" {
		query, err = buildQuery(req)
		if err != nil {
			return fmt.Errorf("could not build query: %s", err)
		}
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return err
	}

	properties := req.Shape.Properties
	valueBuffer := make([]interface{}, len(properties))
	mapBuffer := make(map[string]interface{}, len(properties))

	for rows.Next() {
		if ctx.Err() != nil || !s.connected {
			return nil
		}

		for i, p := range properties {
			switch p.Type {
			case pub.PropertyType_FLOAT:
				var x float64
				valueBuffer[i] = &x
			case pub.PropertyType_INTEGER:
				var x int64
				valueBuffer[i] = &x
			case pub.PropertyType_DECIMAL:
				var x string
				valueBuffer[i] = &x
			default:
				valueBuffer[i] = &valueBuffer[i]
			}
		}
		err = rows.Scan(valueBuffer...)
		if err != nil {
			return err
		}

		for i, p := range properties {
			value := valueBuffer[i]
			mapBuffer[p.Id] = value
		}

		var record *pub.Record
		record, err = pub.NewRecord(pub.Record_UPSERT, mapBuffer)
		if err != nil {
			return err
		}
		out <- record
	}

	return err
}

func buildQuery(req *pub.PublishRequest) (string, error) {

	w := new(strings.Builder)
	w.WriteString("select ")
	if req.Limit > 0 {
		fmt.Fprintf(w, "top(%d) ", req.Limit)
	}
	var columnIDs []string
	for _, p := range req.Shape.Properties {
		columnIDs = append(columnIDs, p.Id)
	}
	columns := strings.Join(columnIDs, ", ")
	w.WriteString(columns)
	w.WriteString("from ")
	w.WriteString(req.Shape.Id)

	return w.String(), nil
}

func (s *Server) PublishStream(req *pub.PublishRequest, stream pub.Publisher_PublishStreamServer) error {

	if !s.connected {
		return errNotConnected
	}

	var err error
	records := make(chan *pub.Record)

	go func() {
		err = s.readRecords(context.Background(), req, records)
	}()

	for record := range records {
		stream.Send(record)
	}

	return err
}

func (s *Server) Disconnect(context.Context, *pub.DisconnectRequest) (*pub.DisconnectResponse, error) {
	if s.db != nil {
		s.db.Close()
	}

	s.connected = false
	s.settings = nil
	s.db = nil

	return new(pub.DisconnectResponse), nil
}

var errNotConnected = errors.New("not connected")

func convertSQLType(t string, maxLength int) pub.PropertyType {
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
	case "bigint",  "decimal", "money", "smallmoney", "numeric":
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

func formatTypeAtSource(t string, maxLength, precision, scale int) string {
	var maxLengthString string
	if maxLength < 0 {
		maxLengthString = "MAX"
	} else {
		maxLengthString = fmt.Sprintf("%d", maxLength)
	}

	switch t {
	case "char", "varchar", "nvarchar", "nchar", "binary", "varbinary", "text", "ntext":
		return fmt.Sprintf("%s(%s)", t, maxLengthString)
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%d,%d)", t, precision, scale)
	case "float", "real":
		return fmt.Sprintf("%s(%d)", t, precision)
	case "datetime2":
		return fmt.Sprintf("%s(%d)", t, scale)
	default:
		return t
	}
}
