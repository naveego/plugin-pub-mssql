package internal

import (
	"github.com/LK4D4/joincontext"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	"io/ioutil"
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
	session *Session
}

type Session struct {
	Ctx        context.Context
	Cancel     func()
	Publishing bool
	Log hclog.Logger
	Settings   *Settings
	// tables that were discovered during connect
	SchemaInfo         map[string]SchemaInfo
	RealTimeHelper *RealTimeHelper
	DB             *sql.DB
}

type SchemaInfo struct {
	IsView bool
}

type OpSession struct {
	Session
	// Cancel cancels the context in this operation.
	Cancel func()
	// Ctx is the context for this operation. It will
	// be done when the context from gRPC call is done,
	// the overall session context is cancelled (by a disconnect)
	// or Cancel is called on this OpSession instance.
	Ctx context.Context
}

func (s *Session) opSession(ctx context.Context) *OpSession {
	ctx, cancel := joincontext.Join(s.Ctx, ctx)
	return &OpSession{
		Session: *s,
		Cancel:cancel,
		Ctx:     ctx,
	}
}

// NewServer creates a new publisher Server.
func NewServer(logger hclog.Logger) pub.PublisherServer {

	manifestBytes, err := ioutil.ReadFile("manifest.json")
	if err != nil {
		panic(errors.Wrap(err, "manifest.json must be in plugin directory"))
	}
	var manifest map[string]interface{}
	err = json.Unmarshal(manifestBytes, &manifest)
	if err != nil {
		panic(errors.Wrap(err, "manifest.json was invalid"))
	}

	configSchema := manifest["configSchema"].(map[string]interface{})
	configSchemaSchema = configSchema["schema"].(map[string]interface{})
	configSchemaUI = configSchema["ui"].(map[string]interface{})
	b, _ := json.Marshal(configSchemaSchema)
	configSchemaSchemaJSON = string(b)
	b, _ = json.Marshal(configSchemaUI)
	configSchemaUIJSON = string(b)

	return &Server{
		mu:  &sync.Mutex{},
		log: logger,
	}
}

var configSchemaUI map[string]interface{}
var configSchemaUIJSON string
var configSchemaSchema map[string]interface{}
var configSchemaSchemaJSON string

// Connect connects to the data base and validates the connections
func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {
	s.log.Debug("Connecting...")
	s.disconnect()

	s.mu.Lock()
	defer s.mu.Unlock()

	session := &Session{
		Log:s.log,
		SchemaInfo: map[string]SchemaInfo{},
	}

	session.Ctx, session.Cancel = context.WithCancel(context.Background())

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

	session.Settings = settings

	session.DB, err = sql.Open("sqlserver", connectionString)
	if err != nil {
		return nil, errors.Errorf("could not open connection: %s", err)
	}

	rows, err := session.DB.Query(`select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
from INFORMATION_SCHEMA.TABLES
order by TABLE_NAME`)
	if err != nil {
		return nil, errors.Errorf("could not read database schema: %s", err)
	}

	// Collect table names for display in UIs.
	for rows.Next() {
		var schema, table, typ string
		err = rows.Scan(&schema, &table, &typ)
		if err != nil {
			return nil, errors.Wrap(err, "could not read table schema")
		}
		id := GetSchemaID(schema, table)
		session.SchemaInfo[id] = SchemaInfo{IsView:typ=="VIEW"}
	}

	s.session = session

	s.log.Debug("Connect completed successfully.")

	return new(pub.ConnectResponse), err
}

func (s *Server) ConnectSession(*pub.ConnectRequest, pub.Publisher_ConnectSessionServer) error {
	panic("not supported")
}

func (s *Server) ConfigureConnection(ctx context.Context, req *pub.ConfigureConnectionRequest) (*pub.ConfigureConnectionResponse, error) {
	return &pub.ConfigureConnectionResponse{
		Form: &pub.ConfigurationFormResponse{
			DataJson:   req.Form.DataJson,
			StateJson:  req.Form.StateJson,
			SchemaJson: configSchemaSchemaJSON,
			UiJson:     configSchemaUIJSON,
		},
	}, nil
}

func (s *Server) ConfigureQuery(context.Context, *pub.ConfigureQueryRequest) (*pub.ConfigureQueryResponse, error) {
	panic("implement me")
}

func (s *Server) ConfigureRealTime(ctx context.Context, req *pub.ConfigureRealTimeRequest) (*pub.ConfigureRealTimeResponse, error) {

	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	if req.Form == nil {
		req.Form = &pub.ConfigurationFormRequest{}
	}

	if session.RealTimeHelper == nil {
		session.RealTimeHelper = new(RealTimeHelper)
	}

	ctx, cancel := context.WithCancel(session.Ctx)
	defer cancel()
	resp, err := session.RealTimeHelper.ConfigureRealTime(session, req)

	return resp, err
}

func (s *Server) BeginOAuthFlow(context.Context, *pub.BeginOAuthFlowRequest) (*pub.BeginOAuthFlowResponse, error) {
	panic("implement me")
}

func (s *Server) CompleteOAuthFlow(context.Context, *pub.CompleteOAuthFlowRequest) (*pub.CompleteOAuthFlowResponse, error) {
	panic("implement me")
}


// DiscoverShapes discovers shapes present in the database
func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverShapesRequest) (*pub.DiscoverShapesResponse, error) {

	s.log.Debug("Handling DiscoverShapesRequest...")

	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	var shapes []*pub.Shape

	if req.Mode == pub.DiscoverShapesRequest_ALL {
		s.log.Debug("Discovering all tables and views...")
		shapes, err = s.getAllShapesFromSchema(session)
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
			err := s.populateShapeColumns(session, shape)
			if err != nil {
				s.log.With("shape", shape.Id).With("err", err).Error("Error discovering columns.")
				shape.Errors = append(shape.Errors, fmt.Sprintf("Could not discover columns: %s", err))
				goto Done
			}
			s.log.Debug("Got details for discovered schema.", "id", shape.Id)

			if req.Mode == pub.DiscoverShapesRequest_REFRESH {
			s.log.Debug("Getting count for discovered schema...", "id", shape.Id)
				shape.Count, err = s.getCount(session, shape)
				if err != nil {
					s.log.With("shape", shape.Id).With("err", err).Error("Error getting row count.")
					shape.Errors = append(shape.Errors, fmt.Sprintf("Could not get row count for shape: %s", err))
					goto Done
				}
				s.log.Debug("Got count for discovered schema.", "id", shape.Id, "count", shape.Count.String())
			}

			if req.SampleSize > 0 {
				s.log.Debug("Getting sample for discovered schema...", "id", shape.Id, "size", req.SampleSize)
				publishReq := &pub.PublishRequest{
					Shape: shape,
					Limit: req.SampleSize,
				}
				records := make(chan *pub.Record)

				go func() {
					err = readRecords(session, publishReq, records)
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

func (s *Server) getAllShapesFromSchema(session *OpSession) ([]*pub.Shape, error) {


	rows, err := session.DB.Query(`SELECT Schema_name(o.schema_id) SchemaName, o.NAME TableName
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
			shape.Id = GetSchemaID(schemaName, tableName)
			shape.Name = tableName
		} else {
			shape.Id = GetSchemaID(schemaName, tableName)
			shape.Name = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		shapes = append(shapes, shape)
	}

	return shapes, nil
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

func (s *Server) populateShapeColumns(session *OpSession, shape *pub.Shape) (error) {

	query := shape.Query
	if query == "" {
		query = fmt.Sprintf("SELECT * FROM %s", shape.Id)
	}

	query = strings.Replace(query, "'", "''", -1)

	metaQuery := fmt.Sprintf("sp_describe_first_result_set N'%s', @params= N'', @browse_information_mode=1", query)

	rows, err := session.DB.Query(metaQuery)

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

	session, err := s.getOpSession(context.Background())
	if err != nil {
		return err
	}

	defer session.Cancel()

	jsonReq, _ := json.Marshal(req)

	s.log.Debug("Got PublishStream request.", "req", string(jsonReq))


	if session.Settings.PrePublishQuery != "" {
		_, err := session.DB.Exec(session.Settings.PrePublishQuery)
		if err != nil {
			return errors.Errorf("error running pre-publish query: %s", err)
		}
	}

	records := make(chan *pub.Record)

	if req.RealTimeSettingsJson == "" {
		go func() {
			defer close(records)
			err = readRecords(session, req, records)
		}()
	} else {
		if session.RealTimeHelper == nil {
			session.RealTimeHelper = &RealTimeHelper{}
		}
		go func() {
			defer close(records)
			err = session.RealTimeHelper.PublishStream(session, req, records)
		}()
	}

	for record := range records {
		sendErr := stream.Send(record)
		if sendErr != nil {
			session.Cancel()
			err = sendErr
			break
		}
	}

	if session.Settings.PostPublishQuery != "" {
		_, postPublishErr := session.DB.Exec(session.Settings.PostPublishQuery)
		if postPublishErr != nil {
			if err != nil {
				postPublishErr = errors.Errorf("%s (publish had already stopped with error: %s)", postPublishErr, err)
			}

			return errors.Errorf("error running post-publish query: %s", postPublishErr)
		}
	}

	return err
}

// Disconnect disconnects from the server
func (s *Server) Disconnect(context.Context, *pub.DisconnectRequest) (*pub.DisconnectResponse, error) {

	s.disconnect()

	return new(pub.DisconnectResponse), nil
}

func (s *Server) disconnect(){
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.session != nil {
		s.session.Cancel()
		if s.session.DB != nil {
			s.session.DB.Close()
		}
	}

	s.session = nil
}

func (s *Server) getOpSession(ctx context.Context) (*OpSession, error){
	s.mu.Lock()
	defer s.mu.Unlock()


	if s.session == nil {
		return nil, errors.New("not connected")
	}

	if s.session.Ctx != nil && s.session.Ctx.Err() != nil {
		return nil, s.session.Ctx.Err()
	}

	return s.session.opSession(ctx), nil
}

func (s *Server) getCount(session *OpSession, shape *pub.Shape) (*pub.Count, error) {

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

			if len(args) == 1 {
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

		row := session.DB.QueryRow(query)
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

func readRecords(session *OpSession, req *pub.PublishRequest, out chan<- *pub.Record) error {

	var query string
	var err error

	query, err = buildQuery(req)
	if err != nil {
		return errors.Errorf("could not build query: %v", err)
	}

	if req.Limit > 0 {
		query = fmt.Sprintf("select top(%d) * from (%s) as q", req.Limit, query)
	}

	stmt, err := session.DB.Prepare(query)
	if err != nil {
		return errors.Errorf("prepare query %q: %s", query, err)
	}
	session.Log.Debug("Prepared query for reading records.", "query", query)
	defer stmt.Close()

	return readRecordsUsingQuery(session, req.Shape, out, stmt)
}

func readRecordsUsingQuery(session *OpSession, shape *pub.Shape, out chan<- *pub.Record, stmt *sql.Stmt, args ...interface{}) error {


	var err error
	rows, err := stmt.Query(args...)
	if err != nil {
		return errors.Errorf("error executing query: %v", err)
	}

	properties := shape.Properties
	valueBuffer := make([]interface{}, len(properties))
	mapBuffer := make(map[string]interface{}, len(properties))

	for rows.Next() {
		if session.Ctx.Err() != nil {
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
