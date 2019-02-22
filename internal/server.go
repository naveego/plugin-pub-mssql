package internal

import (
	"github.com/LK4D4/joincontext"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	"io"
	"io/ioutil"
	"os"
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
	mu      *sync.Mutex
	log     hclog.Logger
	session *Session
	config  *Config
}

type Config struct {
	LogLevel hclog.Level
	// Directory where log files should be stored.
	LogDirectory string
	// Directory where the plugin can store data permanently.
	PermanentDirectory string
	// Directory where the plugin can store temporary information which may be deleted.
	TemporaryDirectory string
}

type Session struct {
	Ctx           context.Context
	Cancel        func()
	Publishing    bool
	Log           hclog.Logger
	Settings      *Settings
	WriteSettings *WriteSettings
	// tables that were discovered during connect
	SchemaInfo       map[string]*meta.Schema
	StoredProcedures []string
	RealTimeHelper   *RealTimeHelper
	Config           Config
	DB               *sql.DB
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
		Cancel:  cancel,
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

func (s *Server) Configure(ctx context.Context, req *pub.ConfigureRequest) (*pub.ConfigureResponse, error) {

	config := &Config{
		LogLevel:           hclog.LevelFromString(req.LogLevel.String()),
		TemporaryDirectory: req.TemporaryDirectory,
		PermanentDirectory: req.PermanentDirectory,
		LogDirectory:       req.LogDirectory,
	}

	s.log.SetLevel(config.LogLevel)

	err := os.MkdirAll(config.PermanentDirectory, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "ensure permanent directory available")
	}

	err = os.MkdirAll(config.TemporaryDirectory, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "ensure temporary directory available")
	}

	s.config = config

	return new(pub.ConfigureResponse), nil
}

func (s *Server) getConfig() (Config, error) {
	if s.config == nil {
		_, err := s.Configure(context.Background(), &pub.ConfigureRequest{
			LogDirectory:       "",
			LogLevel:           pub.LogLevel_Info,
			PermanentDirectory: "./data",
			TemporaryDirectory: "./temp",
		})
		if err != nil {
			return Config{}, err
		}
	}

	return *s.config, nil
}

var configSchemaUI map[string]interface{}
var configSchemaUIJSON string
var configSchemaSchema map[string]interface{}
var configSchemaSchemaJSON string

// Connect connects to the data base and validates the connections
func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {
	var err error
	s.log.Debug("Connecting...")
	s.disconnect()

	s.mu.Lock()
	defer s.mu.Unlock()

	session := &Session{
		Log:        s.log,
		SchemaInfo: map[string]*meta.Schema{},
	}

	session.Config, err = s.getConfig()
	if err != nil {
		return nil, err
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

	rows, err := session.DB.Query(`SELECT t.TABLE_NAME
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
		info, ok := session.SchemaInfo[id]
		if !ok {
			info = &meta.Schema{
				ID:               id,
				IsTable:          typ == "BASE TABLE",
				IsChangeTracking: changeTracking,
			}
			session.SchemaInfo[id] = info
		}
		columnName = fmt.Sprintf("[%s]", columnName)
		columnInfo, ok := info.LookupColumn(columnName)
		if !ok {
			columnInfo = info.AddColumn(&meta.Column{ID: columnName})
		}
		columnInfo.IsKey = constraint != nil && *constraint == "PRIMARY KEY"
	}

	rows, err = session.DB.Query("SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.routines WHERE routine_type = 'PROCEDURE'")
	if err != nil {
		return nil, errors.Errorf("could not read stored procedures from database: %s", err)
	}

	for rows.Next() {
		var schema, name string
		var safeName string
		err = rows.Scan(&schema, &name)
		if err != nil {
			return nil, errors.Wrap(err, "could not read stored procedure schema")
		}
		if schema == "dbo" {
			safeName = makeSQLNameSafe(name)
		} else {
			safeName = fmt.Sprintf("%s.%s", makeSQLNameSafe(schema), makeSQLNameSafe(name))
		}
		session.StoredProcedures = append(session.StoredProcedures, safeName)
	}
	sort.Strings(session.StoredProcedures)

	s.session = session

	s.log.Debug("Connect completed successfully.")

	return new(pub.ConnectResponse), err
}

func (s *Server) ConnectSession(*pub.ConnectRequest, pub.Publisher_ConnectSessionServer) error {
	return errors.New("Not supported.")
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

func (s *Server) ConfigureQuery(ctx context.Context, req *pub.ConfigureQueryRequest) (*pub.ConfigureQueryResponse, error) {
	return nil, errors.New("Not implemented.")
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

func (s *Server) BeginOAuthFlow(ctx context.Context, req *pub.BeginOAuthFlowRequest) (*pub.BeginOAuthFlowResponse, error) {
	return nil, errors.New("Not supported.")
}

func (s *Server) CompleteOAuthFlow(ctx context.Context, req *pub.CompleteOAuthFlowRequest) (*pub.CompleteOAuthFlowResponse, error) {
	return nil, errors.New("Not supported.")
}

func (s *Server) DiscoverSchemas(ctx context.Context, req *pub.DiscoverSchemasRequest) (*pub.DiscoverSchemasResponse, error) {

	s.log.Debug("Handling DiscoverSchemasRequest...")

	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	var schemas []*pub.Schema

	if req.Mode == pub.DiscoverSchemasRequest_ALL {
		s.log.Debug("Discovering all tables and views...")
		schemas, err = s.getAllShapesFromSchema(session)
		s.log.Debug("Discovered tables and views.", "count", len(schemas))

		if err != nil {
			return nil, errors.Errorf("could not load tables and views from SQL: %s", err)
		}
	} else {
		s.log.Debug("Refreshing schemas from request.", "count", len(req.ToRefresh))
		for _, s := range req.ToRefresh {
			schemas = append(schemas, s)
		}
	}

	resp := &pub.DiscoverSchemasResponse{}

	wait := new(sync.WaitGroup)

	for i := range schemas {
		shape := schemas[i]
		// include this shape in wait group
		wait.Add(1)

		// concurrently get details for shape
		go func() {
			s.log.Debug("Getting details for discovered schema", "id", shape.Id)
			err := s.populateShapeColumns(session, shape)
			if err != nil {
				s.log.With("shape", shape.Id).With("err", err).Error("Error discovering columns")
				shape.Errors = append(shape.Errors, fmt.Sprintf("Could not discover columns: %s", err))
				goto Done
			}
			s.log.Debug("Got details for discovered schema", "id", shape.Id)

			if req.Mode == pub.DiscoverSchemasRequest_REFRESH {
				s.log.Debug("Getting count for discovered schema", "id", shape.Id)
				shape.Count, err = s.getCount(session, shape)
				if err != nil {
					s.log.With("shape", shape.Id).With("err", err).Error("Error getting row count.")
					shape.Errors = append(shape.Errors, fmt.Sprintf("Could not get row count for shape: %s", err))
					goto Done
				}
				s.log.Debug("Got count for discovered schema", "id", shape.Id, "count", shape.Count.String())
			} else {
				shape.Count = &pub.Count{Kind: pub.Count_UNAVAILABLE}
			}

			if req.SampleSize > 0 {
				s.log.Debug("Getting sample for discovered schema", "id", shape.Id, "size", req.SampleSize)
				publishReq := &pub.ReadRequest{
					Schema: shape,
					Limit:  req.SampleSize,
				}

				collector := new(RecordCollector)
				handler, innerRequest, err := BuildHandlerAndRequest(session, publishReq, collector)

				err = handler.Handle(innerRequest)

				for _, record := range collector.Records {
					shape.Sample = append(shape.Sample, record)
				}

				if err != nil {
					s.log.With("shape", shape.Id).With("err", err).Error("Error collecting sample.")
					shape.Errors = append(shape.Errors, fmt.Sprintf("Could not collect sample: %s", err))
					goto Done
				}
				s.log.Debug("Got sample for discovered schema", "id", shape.Id, "size", len(shape.Sample))
			}
		Done:
			wait.Done()
		}()
	}

	// wait until all concurrent shape details have been loaded
	wait.Wait()

	for _, schema := range schemas {
		resp.Schemas = append(resp.Schemas, schema)
	}

	sort.Sort(pub.SortableShapes(resp.Schemas))

	return resp, nil
}

func (s *Server) ReadStream(req *pub.ReadRequest, stream pub.Publisher_ReadStreamServer) error {

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

	handler, innerRequest, err := BuildHandlerAndRequest(session, req, PublishToStreamHandler(stream))

	err = handler.Handle(innerRequest)

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

// ConfigureWrite
func (s *Server) ConfigureWrite(ctx context.Context, req *pub.ConfigureWriteRequest) (*pub.ConfigureWriteResponse, error) {
	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	var errArray []string

	storedProcedures, _ := json.Marshal(session.StoredProcedures)
	schemaJSON := fmt.Sprintf(`{
	"type": "object",
	"properties": {
		"storedProcedure": {
			"type": "string",
			"title": "Stored Procedure Name",
			"description": "The name of the stored procedure",
			"enum": %s
		}
	},
	"required": [
		"storedProcedure"
	]
}`, storedProcedures)

	// first request return ui json schema form
	if req.Form == nil || req.Form.DataJson == "" {
		return &pub.ConfigureWriteResponse{
			Form: &pub.ConfigurationFormResponse{
				DataJson:       `{"storedProcedure":""}`,
				DataErrorsJson: "",
				Errors:         nil,
				SchemaJson: schemaJSON ,
				StateJson: "",
			},
			Schema: nil,
		}, nil
	}

	// build schema
	var query string
	var properties []*pub.Property
	var data string
	var row *sql.Row
	var stmt *sql.Stmt
	var rows *sql.Rows
	var sprocSchema, sprocName string

	// get form data
	var formData ConfigureWriteFormData
	if err := json.Unmarshal([]byte(req.Form.DataJson), &formData); err != nil {
		errArray = append(errArray, fmt.Sprintf("error reading form data: %s", err))
		goto Done
	}

	if formData.StoredProcedure == "" {
		goto Done
	}

	sprocSchema, sprocName = decomposeSafeName(formData.StoredProcedure)
	// check if stored procedure exists
	query = `SELECT 1
FROM information_schema.routines
WHERE routine_type = 'PROCEDURE'
AND SPECIFIC_SCHEMA = @schema
AND SPECIFIC_NAME = @name
`
	stmt, err = session.DB.Prepare(query)
	if err != nil {
		errArray = append(errArray, fmt.Sprintf("error checking stored procedure: %s", err))
		goto Done
	}

	row = stmt.QueryRow(sql.Named("schema", sprocSchema), sql.Named("name", sprocName))

	err = row.Scan(&data)
	if err != nil {
		errArray = append(errArray, fmt.Sprintf("stored procedure does not exist: %s", err))
		goto Done
	}

	// get params for stored procedure
	query = `SELECT PARAMETER_NAME AS Name, DATA_TYPE AS Type
FROM INFORMATION_SCHEMA.PARAMETERS
WHERE SPECIFIC_SCHEMA = @schema
AND SPECIFIC_NAME = @name
`
	stmt, err = session.DB.Prepare(query)
	if err != nil {
		errArray = append(errArray, fmt.Sprintf("error preparing to get parameters for stored procedure: %s", err))
		goto Done
	}

	rows, err = stmt.Query(sql.Named("schema", sprocSchema), sql.Named("name", sprocName))
	if err != nil {
		errArray = append(errArray, fmt.Sprintf("error getting parameters for stored procedure: %s", err))
		goto Done
	}


	// add all params to properties of schema
	for rows.Next() {
		var colName, colType string
		err := rows.Scan(&colName, &colType)
		if err != nil {
			errArray = append(errArray, fmt.Sprintf("error getting parameters for stored procedure: %s", err))
			goto Done
		}

		properties = append(properties, &pub.Property{
			Id: strings.TrimPrefix(colName, "@"),
			TypeAtSource: colType,
			Type: convertSQLType(colType, 0),
		})
	}

Done:
	// return write back schema
	return &pub.ConfigureWriteResponse{
		Form: &pub.ConfigurationFormResponse{
			DataJson:  req.Form.DataJson,
			Errors:    errArray,
			StateJson: req.Form.StateJson,
			SchemaJson:schemaJSON,
		},
		Schema: &pub.Schema{
			Id:         formData.StoredProcedure,
			Query:      formData.StoredProcedure,
			DataFlowDirection: pub.Schema_WRITE,
			Properties: properties,
		},
	}, nil
}

type ConfigureWriteFormData struct {
	StoredProcedure string `json:"storedProcedure,omitempty"`
}

// PrepareWrite sets up the plugin to be able to write back
func (s *Server) PrepareWrite(ctx context.Context, req *pub.PrepareWriteRequest) (*pub.PrepareWriteResponse, error) {
	// session, err := s.getOpSession(ctx)
	// if err != nil {
	// 	return nil, err
	// }

	s.session.WriteSettings = &WriteSettings{
		Schema:    req.Schema,
		CommitSLA: req.CommitSlaSeconds,
	}

	return &pub.PrepareWriteResponse{}, nil
}

// WriteStream writes a stream of records back to the source system
func (s *Server) WriteStream(stream pub.Publisher_WriteStreamServer) error {
	session, err := s.getOpSession(context.Background())
	if err != nil {
		return err
	}

	defer session.Cancel()

	// get and process each record
	for {
		// return if not configured
		if session.WriteSettings == nil {
			return nil
		}

		// get record and exit if no more records or error
		record, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var recordData map[string]interface{}
		if err := json.Unmarshal([]byte(record.DataJson), &recordData); err != nil {
			return errors.WithStack(err)
		}

		// process record and send ack
		c1 := make(chan string, 1)
		go func() {
			schema := session.WriteSettings.Schema

			// build params for stored procedure
			var args []interface{}
			for _, prop := range schema.Properties {
				args = append(args, sql.Named(prop.Id, recordData[prop.Id]))
			}

			// call stored procedure and capture any error
			_, err := session.DB.Exec(schema.Query, args...)
			if err != nil {
				c1 <- fmt.Sprintf("could not write back: %s", err)
			}

			c1 <- ""
		}()

		// send ack when done writing or on timeout
		select {
		// done writing
		case sendErr := <-c1:
			err := stream.Send(&pub.RecordAck{
				CorrelationId: record.CorrelationId,
				Error:         sendErr,
			})
			if err != nil {
				return err
			}
		// timeout
		case <-time.After(time.Duration(session.WriteSettings.CommitSLA) * time.Second):
			err := stream.Send(&pub.RecordAck{
				CorrelationId: record.CorrelationId,
				Error:         "timed out",
			})
			if err != nil {
				return err
			}
		}
	}
}

// DiscoverShapes discovers shapes present in the database
func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverSchemasRequest) (*pub.DiscoverSchemasResponse, error) {
	return s.DiscoverSchemas(ctx, req)
}

func (s *Server) getAllShapesFromSchema(session *OpSession) ([]*pub.Schema, error) {

	rows, err := session.DB.Query(`SELECT Schema_name(o.schema_id) SchemaName, o.NAME TableName
FROM   sys.objects o 
WHERE  o.type IN ( 'U', 'V' )`)

	if err != nil {
		return nil, errors.Errorf("could not list tables: %s", err)
	}

	var shapes []*pub.Schema

	for rows.Next() {
		shape := new(pub.Schema)

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

func describeResultSet(session *OpSession, query string) ([]describeResult, error) {
	metaQuery := fmt.Sprintf("sp_describe_first_result_set N'%s', @params= N'', @browse_information_mode=1", query)

	rows, err := session.DB.Query(metaQuery)

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

func (s *Server) populateShapeColumns(session *OpSession, shape *pub.Schema) error {

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
func (s *Server) PublishStream(req *pub.ReadRequest, stream pub.Publisher_PublishStreamServer) error {
	return s.ReadStream(req, stream)
}

// Disconnect disconnects from the server
func (s *Server) Disconnect(ctx context.Context, req *pub.DisconnectRequest) (*pub.DisconnectResponse, error) {

	s.disconnect()

	return new(pub.DisconnectResponse), nil
}

func (s *Server) disconnect() {
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

func (s *Server) getOpSession(ctx context.Context) (*OpSession, error) {
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

var schemaIDParseRE = regexp.MustCompile(`(?:\[([^\]]+)\].)?(?:)(?:\[([^\]]+)\])`)

func (s *Server) getCount(session *OpSession, shape *pub.Schema) (*pub.Count, error) {

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
		return nil, fmt.Errorf("error from query %q: %s", query, err)
	}

	return &pub.Count{
		Kind:  pub.Count_EXACT,
		Value: int32(count),
	}, nil
}

func buildQuery(req *pub.ReadRequest) (string, error) {

	if req.Schema.Query != "" {
		return req.Schema.Query, nil
	}

	w := new(strings.Builder)
	w.WriteString("select ")
	var columnIDs []string
	for _, p := range req.Schema.Properties {
		columnIDs = append(columnIDs, p.Id)
	}
	columns := strings.Join(columnIDs, ", ")
	fmt.Fprintln(w, columns)
	fmt.Fprintln(w, "from ", req.Schema.Id)

	if len(req.Filters) > 0 {
		fmt.Fprintln(w, "where")

		properties := make(map[string]*pub.Property, len(req.Schema.Properties))
		for _, p := range req.Schema.Properties {
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

func decomposeSafeName(safeName string) (schema, name string) {
	segs := strings.Split(safeName, ".")
	switch len(segs) {
	case 0:
		return "", ""
	case 1:
		return "dbo", strings.Trim(segs[0], "[]")
	case 2:
		return strings.Trim(segs[0], "[]"), strings.Trim(segs[1], "[]")
	default:
		return "", ""
	}
}

func makeSQLNameSafe(name string) string {
	if ok, _ := regexp.MatchString(`\W`, name); !ok {
		return name
	}
	return fmt.Sprintf("[%s]", name)
}
