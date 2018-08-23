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
		return nil, errors.WithStack(err)
	}

	if err := settings.Validate(); err != nil {
		return nil, errors.WithStack(err)
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
		return nil, errors.Errorf("could not open connection: %s", err)
	}

	_, err = s.db.Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ")

	if err != nil {
		return nil, errors.Errorf("could not read database schema: %s", err)
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

	var shapes []*pub.Shape
	var err error

	if req.Mode == pub.DiscoverShapesRequest_ALL {
		shapes, err = s.getAllShapesFromSchema()
		if err != nil {
			return nil, errors.Errorf("could not load shapes from SQL: %s", err)
		}
	} else {
		for _, s := range req.ToRefresh {
			shapes = append(shapes, s)
		}
	}

	resp := &pub.DiscoverShapesResponse{}

	for _, shape := range shapes {

		err := s.populateShapeColumns(shape)
		if err != nil {
			return resp, errors.Errorf("could not populate columns for shape %q: %s", shape.Id, err)
		}
	}

	for _, shape := range shapes {
		sort.Sort(pub.SortableProperties(shape.Properties))

		shape.Count, err = s.getCount(shape)
		if err != nil {
			return nil, errors.Errorf("could not get row count for shape %q: %s", shape.Id, err)
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
				return nil, errors.Errorf("error while collecting sample: %v", err)
			}
		}

		resp.Shapes = append(resp.Shapes, shape)
	}

	sort.Sort(pub.SortableShapes(resp.Shapes))

	return resp, nil

}

func (s *Server) getAllShapesFromSchema() ([]*pub.Shape, error) {

	rows, err := s.db.Query(`SELECT Schema_name(o.schema_id) SchemaName, o.NAME TableName
FROM   sys.objects o 
WHERE  o.type IN ( 'U', 'V' )`)

	if err != nil {
		return nil, errors.Errorf("could not list tables: %s", err)
	}

	var shapes []*pub.Shape

	for rows.Next() {
		shape := new(pub.Shape)

		var (
			schemaName string
			tableName  string
		)
		err = rows.Scan(&schemaName, &tableName)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if schemaName == "dbo" {
			shape.Id = fmt.Sprintf("[%s]", tableName)
			shape.Name = tableName
		} else {
			shape.Id = fmt.Sprintf("[%s].[%s]", schemaName, tableName)
			shape.Name = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		shapes = append(shapes, shape)
	}

	return shapes, nil
}

func (s *Server) populateShapeColumns(shape *pub.Shape) (error) {

	query := shape.Query
	if query == "" {
		query = fmt.Sprintf("SELECT * FROM %s", shape.Id)
	}

	query = strings.Replace(query, "'", "''", -1)

	metaQuery := fmt.Sprintf("sp_describe_first_result_set N'%s', @params= N'', @browse_information_mode=1", query)

	rows, err := s.db.Query(metaQuery)

	if err != nil {
		return errors.Errorf("error executing query %q: %v", metaQuery, err)
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return errors.WithStack(err)
	}

	unnamedColumnIndex := 1

	for rows.Next() {

		columns := make([]interface{}, len(columnNames))
		columnPointers := make([]interface{}, len(columnNames))
		columnMap := make(map[string]interface{}, len(columnNames))
		for i := 0; i < len(columnNames); i++ {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return errors.WithStack(err)
		}
		for i, name := range columnNames {
			columnMap[name] = columns[i]
		}

		var property *pub.Property
		var propertyName string
		var propertyID string
		var ok bool
		if propertyName, ok = columnMap["name"].(string); !ok {
			propertyName = fmt.Sprintf("UNKNOWN_%d", unnamedColumnIndex)
			unnamedColumnIndex++
		}
		propertyID = fmt.Sprintf("[%s]", propertyName)

		for _, p := range shape.Properties {
			if p.Id == propertyID {
				property = p
				break
			}
		}
		if property == nil {
			property = &pub.Property{
				Id:   propertyID,
				Name: propertyName,
			}
			shape.Properties = append(shape.Properties, property)
		}

		if property.TypeAtSource, ok = columnMap["system_type_name"].(string); !ok {
			property.TypeAtSource = "binary"
		}

		maxLength := columnMap["max_length"].(int64)
		property.Type = convertSQLType(property.TypeAtSource, int(maxLength))

		if property.IsNullable, ok = columnMap["is_nullable"].(bool); !ok {
			property.IsNullable = true
		}

		if property.IsKey, ok = columnMap["is_part_of_unique_key"].(bool); !ok {
			property.IsKey = false
		}
	}

	return nil
}

func (s *Server) PublishStream(req *pub.PublishRequest, stream pub.Publisher_PublishStreamServer) error {

	jsonReq, _ := json.Marshal(req)

	s.log.With("req", string(jsonReq)).Debug("Got PublishStream request.")

	if !s.connected {
		return errNotConnected
	}

	if s.settings.PrePublishQuery != "" {
		_, err := s.db.Exec(s.settings.PrePublishQuery)
		if err != nil {
			return errors.Errorf("error running pre-publish query: %s", err)
		}
	}

	var err error
	records := make(chan *pub.Record)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err = s.readRecords(ctx, req, records)
	}()

	for record := range records {
		sendErr := stream.Send(record)
		if sendErr != nil {
			cancel()
			err = sendErr
			break
		}
	}

	if s.settings.PostPublishQuery != "" {
		_, postPublishErr := s.db.Exec(s.settings.PostPublishQuery)
		if postPublishErr != nil {
			if err != nil {
				postPublishErr = errors.Errorf("%s (publish had already stopped with error: %s)", postPublishErr, err)
			}

			return errors.Errorf("error running post-publish query: %s", postPublishErr)
		}
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

func (s *Server) getCount(shape *pub.Shape) (*pub.Count, error) {

	var count int

	query, err := buildQuery(&pub.PublishRequest{
		Shape:shape,
	})
	if err != nil {
		return nil, err
	}

	query = fmt.Sprintf("SELECT COUNT(1) FROM (%s) as Q", query)

	row := s.db.QueryRow(query)

	err = row.Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("error from query %q: %s", query,  err)
	}

	return &pub.Count{
		Kind:  pub.Count_EXACT,
		Value: int32(count),
	}, nil
}

func (s *Server) readRecords(ctx context.Context, req *pub.PublishRequest, out chan<- *pub.Record) error {

	defer close(out)

	var err error
	var query string

	query, err = buildQuery(req)
	if err != nil {
		return errors.Errorf("could not build query: %v", err)
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return errors.Errorf("error executing query %q: %v", query, err)
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
			return errors.WithStack(err)
		}

		for i, p := range properties {
			value := valueBuffer[i]
			mapBuffer[p.Id] = value
		}

		var record *pub.Record
		record, err = pub.NewRecord(pub.Record_UPSERT, mapBuffer)
		if err != nil {
			return errors.WithStack(err)
		}
		out <- record
	}

	return err
}

func buildQuery(req *pub.PublishRequest) (string, error) {

	if req.Shape.Query != "" {
		return req.Shape.Query, nil
	}

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
	fmt.Fprintln(w, columns)
	fmt.Fprintln(w, "from ", req.Shape.Id)

	if len(req.Filters) > 0 {
		fmt.Fprintln(w, "where")

		properties := make(map[string]*pub.Property, len(req.Shape.Properties))
		for _, p := range req.Shape.Properties {
			properties[p.Id] = p
		}

		var filters []string
		for _, f := range req.Filters {
			property, ok := properties[f.PropertyId]
			if !ok {
				continue
			}

			wf := new(strings.Builder)

			fmt.Fprintf(wf, "  %s ", f.PropertyId)
			switch f.Kind {
			case pub.PublishFilter_EQUALS:
				fmt.Fprint(wf, "= ")
			case pub.PublishFilter_GREATER_THAN:
				fmt.Fprint(wf, "> ")
			case pub.PublishFilter_LESS_THAN:
				fmt.Fprint(wf, "< ")
			default:
				continue
			}

			switch property.Type {
			case pub.PropertyType_INTEGER, pub.PropertyType_FLOAT:
				fmt.Fprintf(wf, "%v", f.Value)
			default:
				fmt.Fprintf(wf, "CAST('%s' as %s)", f.Value, property.TypeAtSource)
			}

			filters = append(filters, wf.String())
		}

		fmt.Fprintln(w, strings.Join(filters, "AND\n  "))

	}

	return w.String(), nil
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
