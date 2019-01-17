package internal

import (
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	"sync"
	"time"

	"context"

	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"regexp"
	"sort"
	"strings"
)

// Server type to describe a server
type Server struct {
	mu         *sync.Mutex
	log        hclog.Logger
	settings   *Settings
	db         *sql.DB
	publishing bool
	connected  bool
}

// NewServer creates a new publisher Server.
func NewServer(logger hclog.Logger) pub.PublisherServer {
	return &Server{
		mu:  &sync.Mutex{},
		log: logger,
	}
}

// Connect connects to the data base and validates the connections
func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {
	s.log.Debug("Connecting...")
	s.settings = nil
	s.connected = false

	settings := new(Settings)
	if err := json.Unmarshal([]byte(req.SettingsJson), settings); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := settings.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	connectionString, err := settings.GetConnectionString()
	if err != nil {
		return nil, err
	}

	s.db, err = sql.Open("sqlserver", connectionString)
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

	s.log.Debug("Connect completed successfully.")

	return new(pub.ConnectResponse), err
}

// DiscoverShapes discovers shapes present in the database
func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverShapesRequest) (*pub.DiscoverShapesResponse, error) {

	s.log.Debug("Handling DiscoverShapesRequest...")

	if !s.connected {
		return nil, errNotConnected
	}

	var shapes []*pub.Shape
	var err error

	if req.Mode == pub.DiscoverShapesRequest_ALL {
		s.log.Debug("Discovering all tables and views...")
		shapes, err = s.getAllShapesFromSchema()
		s.log.Debug("Discovered tables and views.", "count", len(shapes))

		if err != nil {
			return nil, errors.Errorf("could not load tables and views from SQL: %s", err)
		}
	} else {
		s.log.Debug("Refreshing schemas from request.", "count", len(req.ToRefresh))
		for _, s := range req.ToRefresh {
			shapes = append(shapes, s)
		}
	}

	resp := &pub.DiscoverShapesResponse{}

	wait := new(sync.WaitGroup)

	for i := range shapes {
		shape := shapes[i]
		// include this shape in wait group
		wait.Add(1)

		// concurrently get details for shape
		go func() {
			s.log.Debug("Getting details for discovered schema...", "id", shape.Id)
			err := s.populateShapeColumns(shape)
			if err != nil {
				s.log.With("shape", shape.Id).With("err", err).Error("Error discovering columns.")
				shape.Errors = append(shape.Errors, fmt.Sprintf("Could not discover columns: %s", err))
				goto Done
			}
			s.log.Debug("Got details for discovered schema.", "id", shape.Id)

			s.log.Debug("Getting count for discovered schema...", "id", shape.Id)
			shape.Count, err = s.getCount(shape)
			if err != nil {
				s.log.With("shape", shape.Id).With("err", err).Error("Error getting row count.")
				shape.Errors = append(shape.Errors, fmt.Sprintf("Could not get row count for shape: %s", err))
				goto Done
			}
			s.log.Debug("Got count for discovered schema.", "id", shape.Id, "count", shape.Count.String())

			if req.SampleSize > 0 {
				s.log.Debug("Getting sample for discovered schema...", "id", shape.Id, "size", req.SampleSize)
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
					s.log.With("shape", shape.Id).With("err", err).Error("Error collecting sample.")
					shape.Errors = append(shape.Errors, fmt.Sprintf("Could not collect sample: %s", err))
					goto Done
				}
				s.log.Debug("Got sample for discovered schema.", "id", shape.Id, "size", len(shape.Sample))
			}
		Done:
			wait.Done()
		}()
	}

	// wait until all concurrent shape details have been loaded
	wait.Wait()

	for _, shape := range shapes {
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

type describeResult struct {
	IsHidden          bool   `sql:"is_hidden"`
	Name              string `sql:"name"`
	SystemTypeName    string `sql:"system_type_name"`
	IsNullable        bool   `sql:"is_nullable"`
	IsPartOfUniqueKey bool   `sql:"is_part_of_unique_key"`
	MaxLength         int64  `sql:"max_length"`
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

	if err != nil {
		return errors.WithStack(err)
	}

	metadata := make([]describeResult, 0, 0)

	err = sqlstructs.UnmarshalRows(rows, &metadata)
	if err != nil {
		return err
	}

	unnamedColumnIndex := 0

	for _, m := range metadata {

		if m.IsHidden {
			continue
		}

		var property *pub.Property
		var propertyID string

		propertyName := m.Name
		if propertyName == "" {
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

		property.TypeAtSource = m.SystemTypeName

		maxLength := m.MaxLength
		property.Type = convertSQLType(property.TypeAtSource, int(maxLength))

		property.IsNullable = m.IsNullable
		property.IsKey = m.IsPartOfUniqueKey
	}

	return nil
}

// PublishStream sends records read in request to the agent
func (s *Server) PublishStream(req *pub.PublishRequest, stream pub.Publisher_PublishStreamServer) error {

	jsonReq, _ := json.Marshal(req)

	s.log.Debug("Got PublishStream request.", "req", string(jsonReq))

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

			cancel()
			return errors.Errorf("error running post-publish query: %s", postPublishErr)
		}
	}

	cancel()
	return err
}

// Disconnect disconnects from the server
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

	cErr := make(chan error)
	cCount := make(chan int)

	go func() {
		defer close(cErr)
		defer close(cCount)

		var query string
		var err error

		if shape.Query != "" {
			query = fmt.Sprintf("SELECT COUNT(1) FROM (%s) as Q", shape.Query)
		} else {
			r, err := regexp.Compile(`\[.*?\]`)
			if err != nil {
				cErr <- fmt.Errorf("error from regexp: %s", err)
				return
			}

			args := r.FindAllString(shape.Id, -1)
			
			if (len(args) == 1) {
				args = append(args, args[0])
				args[0] = "[dbo]"
			}

			args[0] = strings.TrimPrefix(args[0], "[")
			args[0] = strings.TrimSuffix(args[0], "]")
			args[1] = strings.TrimPrefix(args[1], "[")
			args[1] = strings.TrimSuffix(args[1], "]")

			query = fmt.Sprintf(`
			SELECT SUM(p.rows) FROM sys.partitions AS p
			INNER JOIN sys.tables AS t
			ON p.[object_id] = t.[object_id]
			INNER JOIN sys.schemas AS s
			ON s.[schema_id] = t.[schema_id]
			WHERE t.name = N'%s'
			AND s.name = N'%s'
			AND p.index_id IN (0,1);`, args[1], args[0])
		}

		row := s.db.QueryRow(query)
		var count int
		err = row.Scan(&count)
		if err != nil {
			cErr <- fmt.Errorf("error from query %q: %s", query, err)
			return
		}

		cCount <- count
	}()

	select {
	case err := <-cErr:
		return nil, err
	case count := <-cCount:
		return &pub.Count{
			Kind:  pub.Count_EXACT,
			Value: int32(count),
		}, nil
	case <-time.After(time.Second):
		return &pub.Count{
			Kind: pub.Count_UNAVAILABLE,
		}, nil
	}
}

func (s *Server) readRecords(ctx context.Context, req *pub.PublishRequest, out chan<- *pub.Record) error {

	defer close(out)

	var err error
	var query string

	query, err = buildQuery(req)
	if err != nil {
		return errors.Errorf("could not build query: %v", err)
	}

	if req.Limit > 0 {
		query = fmt.Sprintf("select top(%d) * from (%s) as q", req.Limit, query)
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
				var x *float64
				valueBuffer[i] = &x
			case pub.PropertyType_INTEGER:
				var x *int64
				valueBuffer[i] = &x
			case pub.PropertyType_DECIMAL:
				var x *string
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
