package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"sync"

	"github.com/LK4D4/joincontext"
	"github.com/avast/retry-go"
	"github.com/hashicorp/go-hclog"
	jsonschema "github.com/naveego/go-json-schema"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
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
	Ctx        context.Context
	Cancel     func()
	Publishing bool
	Log        hclog.Logger
	Settings   *Settings
	Writer     Writer
	// tables that were discovered during connect
	SchemaInfo       map[string]*meta.Schema
	StoredProcedures []string
	RealTimeHelper   *RealTimeHelper
	Config           Config
	DB               *sql.DB
	DbHandles        map[string]*sql.DB
	SchemaDiscoverer SchemaDiscoverer
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

func (s *Session) OpSession(ctx context.Context) *OpSession {
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

func ExtractIPFromWsarecvErr(input string) string {
	matches := wsarecvIPExtractorRE.FindStringSubmatch(input)
	if len(matches) == 2 {
		return matches[1]
	}
	return ""
}

var wsarecvIPExtractorRE = regexp.MustCompile(`(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d+: wsarecv`)

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

	originalHost := settings.Host

	// retry start
	err = retry.Do(
		func() error {
			connectionString, err := settings.GetConnectionString()
			if err != nil {
				return err
			}
			session.Settings = settings
			session.DB, err = sql.Open("sqlserver", connectionString)
			if err != nil {
				return errors.Errorf("could not open connection: %s", err)
			}
			err = session.DB.Ping()
			return err
		},
		retry.RetryIf(func(err error) bool {
			if strings.Contains(err.Error(), "wsarecv") {
				var ip = ExtractIPFromWsarecvErr(err.Error())
				if ip == "" {
					return false
				}

				settings.Host = ip

				return true
				//return s.Connect(ctx, pub.NewConnectRequest(settings))
			}

			return false
			//return nil, errors.Errorf("could not read database schema: %s", err)
		}),
		retry.Attempts(2))

	// retry end
	if err != nil {
		if originalHost != settings.Host {
			return nil, errors.Wrapf(err, "tried original host %q and raw IP %q", originalHost, settings.Host)
		}
		return nil, errors.Wrapf(err, "tried using host %q, port %d", settings.Host, settings.Port)
	}

	rows, err := session.DB.Query(`SELECT t.TABLE_NAME
     , t.TABLE_SCHEMA
     , t.TABLE_TYPE
     , c.COLUMN_NAME
	   , c.DATA_TYPE
     , c.IS_NULLABLE
     , c.CHARACTER_MAXIMUM_LENGTH
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
			schema, table, typ, columnName, dataType, isNullable string
			maxLength                                            sql.NullInt64
			constraint                                           *string
			changeTracking                                       bool
		)
		err = rows.Scan(&table, &schema, &typ, &columnName, &dataType, &isNullable, &maxLength, &constraint, &changeTracking)
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
		columnInfo.IsKey = columnInfo.IsKey || constraint != nil && *constraint == "PRIMARY KEY"
		columnInfo.IsDiscovered = true
		columnInfo.SQLType = dataType
		columnInfo.IsNullable = strings.ToUpper(isNullable) == "YES"
		if maxLength.Valid {
			columnInfo.MaxLength = maxLength.Int64
			columnInfo.SQLType = fmt.Sprintf("%s(%v)", dataType, maxLength.Int64)
			if maxLength.Int64 == -1 {
				columnInfo.SQLType = fmt.Sprintf("%s(max)", dataType)
			}
		} else {
			columnInfo.MaxLength = 0
		}
	}

	rows, err = session.DB.Query("SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE routine_type = 'PROCEDURE'")
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

	session.SchemaDiscoverer = SchemaDiscoverer{
		Log: s.log.With("cmp", "SchemaDiscoverer"),
	}

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

func (s *Server) DiscoverSchemasStream(req *pub.DiscoverSchemasRequest, stream pub.Publisher_DiscoverSchemasStreamServer) error {
	s.log.Debug("Handling DiscoverSchemasStream...")

	session, err := s.getOpSession(stream.Context())
	if err != nil {
		return err
	}

	discovered, err := s.session.SchemaDiscoverer.DiscoverSchemas(session, req)
	if err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()

		case schema, more := <-discovered:
			if !more {
				s.log.Info("Reached end of schema stream.")
				return nil
			}

			s.log.Debug("Discovered schema.", "schema", schema.Name)

			err = stream.Send(schema)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) DiscoverSchemas(ctx context.Context, req *pub.DiscoverSchemasRequest) (*pub.DiscoverSchemasResponse, error) {

	s.log.Debug("Handling DiscoverSchemasRequest...")

	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	schemas, err := DiscoverSchemasSync(session, session.SchemaDiscoverer, req)

	return &pub.DiscoverSchemasResponse{
		Schemas: schemas,
	}, err
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

	handler, innerRequest, err := BuildHandlerAndRequest(session, req, PublishToStreamHandler(session.Ctx, stream))
	if err != nil {
		return errors.Wrap(err, "create handler")
	}

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

	if err != nil && session.Ctx.Err() != nil {
		s.log.Error("Handler returned error, but context was cancelled so error will not be returned to caller. Error was: %s", err.Error())
		return nil
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
				SchemaJson:     schemaJSON,
				StateJson:      "",
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

	_, sprocSchema, sprocName = DecomposeSafeName(formData.StoredProcedure)
	// check if stored procedure exists
	query = `SELECT 1
FROM INFORMATION_SCHEMA.ROUTINES
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
			Id:           strings.TrimPrefix(colName, "@"),
			TypeAtSource: colType,
			Type:         meta.ConvertSQLTypeToPluginType(colType, 0),
			Name:         strings.TrimPrefix(colName, "@"),
		})
	}

Done:
	// return write back schema
	return &pub.ConfigureWriteResponse{
		Form: &pub.ConfigurationFormResponse{
			DataJson:   req.Form.DataJson,
			Errors:     errArray,
			StateJson:  req.Form.StateJson,
			SchemaJson: schemaJSON,
		},
		Schema: &pub.Schema{
			Id:                formData.StoredProcedure,
			Query:             formData.StoredProcedure,
			DataFlowDirection: pub.Schema_WRITE,
			Properties:        properties,
		},
	}, nil
}

type ConfigureWriteFormData struct {
	StoredProcedure string `json:"storedProcedure,omitempty"`
}

func (s *Server) ConfigureReplication(ctx context.Context, req *pub.ConfigureReplicationRequest) (resp *pub.ConfigureReplicationResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s: %s", r, string(debug.Stack()))
			s.log.Error("panic", "error", err, "stackTrace", string(debug.Stack()))
		}
	}()

	return s.configureReplication(ctx, req)
}

func (s *Server) configureReplication(ctx context.Context, req *pub.ConfigureReplicationRequest) (*pub.ConfigureReplicationResponse, error) {
	builder := pub.NewConfigurationFormResponseBuilder(req.Form)

	s.log.Debug("Handling configure replication request.")

	var settings ReplicationSettings
	if req.Form.DataJson != "" {
		if err := json.Unmarshal([]byte(req.Form.DataJson), &settings); err != nil {
			return nil, errors.Wrapf(err, "invalid data json %q", req.Form.DataJson)
		}

		s.log.Debug("Configure replication request had data.", "data", string(req.Form.DataJson))

		if req.Schema != nil {
			s.log.Debug("Configure replication request had a schema.", "schema", req.Schema)
		}
		if req.Form.IsSave {
			s.log.Debug("Configure replication request was a save.")
		}

		for i, a := range settings.PropertyConfiguration {
			for _, b := range settings.PropertyConfiguration[i+1:] {
				if a.Name == b.Name {
					return nil, errors.Errorf("Duplicate property: %s detected", a.Name)
				}
			}
		}

		// No longer committing the configuration on save,
		// it's too destructive and dangerous, and we don't have enough
		// versioning information to do it correctly.

		// if settings.SQLSchema != "" &&
		// 	settings.VersionRecordTable != "" &&
		// 	settings.GoldenRecordTable != "" &&
		// 	req.Schema != nil &&
		// 	req.Form.IsSave {
		//
		// 	s.log.Info("Configure replication request had IsSave=true, committing replication settings to database.")
		//
		// 	// The settings have been filled in, let's make sure it's ready to go.
		//
		// 	session, err := s.getOpSession(ctx)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		//
		// 	_, err = PrepareWriteHandler(session, &pub.PrepareWriteRequest{
		// 		Schema: req.Schema,
		// 		Replication: &pub.ReplicationWriteRequest{
		// 			SettingsJson: req.Form.DataJson,
		// 			Versions:     req.Versions,
		// 		},
		// 	})
		// 	if err != nil {
		// 		s.log.Error("Configuring replication failed.", "req", string(req.Form.DataJson), "err", err)
		// 		builder.Response.Errors = []string{err.Error()}
		// 	}
		// }
	}

	builder.FormSchema = jsonschema.NewGenerator().WithRoot(ReplicationSettings{}).MustGenerate()

	if req.Schema != nil {
		var nameEnum []string
		for _, property := range req.Schema.Properties {
			nameEnum = append(nameEnum, property.Name)
		}

		builder.UISchema = map[string]interface{}{
			"ui:order": []string{"sqlSchema", "goldenRecordTable", "versionRecordTable", "propertyConfig"},
		}
		builder.FormSchema.Properties["propertyConfig"].Items.Properties["name"].Enum = nameEnum
	}

	return &pub.ConfigureReplicationResponse{
		Form: builder.Build(),
	}, nil
}

// PrepareWrite sets up the plugin to be able to write back
func (s *Server) PrepareWrite(ctx context.Context, req *pub.PrepareWriteRequest) (*pub.PrepareWriteResponse, error) {
	session, err := s.getOpSession(ctx)
	if err != nil {
		return nil, err
	}

	s.session.Writer, err = PrepareWriteHandler(session, req)

	if err != nil {
		return nil, err
	}

	schemaJSON, _ := json.MarshalIndent(req.Schema, "", "  ")
	s.log.Debug("Prepared to write.", "commitSLA", req.CommitSlaSeconds, "schema", string(schemaJSON))

	return &pub.PrepareWriteResponse{}, nil
}

// WriteStream writes a stream of records back to the source system
func (s *Server) WriteStream(stream pub.Publisher_WriteStreamServer) error {

	session, err := s.getOpSession(stream.Context())
	if err != nil {
		return err
	}

	if session.Writer == nil {
		return errors.New("session.Writer was nil, PrepareWrite should have been called before WriteStream")
	}

	defer session.Cancel()

	// get and process each record
	for {

		if session.Ctx.Err() != nil {
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

		unmarshalledRecord, err := record.AsUnmarshalled()

		// if the record unmarshalled correctly,
		// we send it to the writer
		if err == nil {
			err = session.Writer.Write(session, unmarshalledRecord)
		}

		if err != nil {
			// send failure ack to agent
			ack := &pub.RecordAck{
				CorrelationId: record.CorrelationId,
				Error:         err.Error(),
			}
			err := stream.Send(ack)
			if err != nil {
				s.log.Error("Error writing error ack to agent.", "err", err, "ack", ack)
				return err
			}
		} else {
			// send success ack to agent
			err := stream.Send(&pub.RecordAck{
				CorrelationId: record.CorrelationId,
			})
			if err != nil {
				s.log.Error("Error writing success ack to agent.", "err", err)
				return err
			}
		}
	}
}

// DiscoverShapes discovers shapes present in the database
func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverSchemasRequest) (*pub.DiscoverSchemasResponse, error) {
	return s.DiscoverSchemas(ctx, req)
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
			err := s.session.DB.Close()
			if err != nil {
				s.log.Error("Error closing connection", "err", err)
			}
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

	return s.session.OpSession(ctx), nil
}

var schemaIDParseRE = regexp.MustCompile(`(?:\[([^\]]+)\].)?(?:)(?:\[([^\]]+)\])`)

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

func makeSQLNameSafe(name string) string {
	if ok, _ := regexp.MatchString(`\W`, name); !ok {
		return name
	}
	return fmt.Sprintf("[%s]", name)
}
