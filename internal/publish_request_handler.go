package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/internal/templates"
	"github.com/naveego/plugin-pub-mssql/pkg/store"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

const (
	ChangeKeyPrefix           = "NAVEEGO_CHANGE_KEY_"
	DeletedFlagColumnName     = "NAVEEGO_DELETED_COLUMN_FLAG"
	ChangeOperationColumnName = "NAVEEGO_CHANGE_OPERATION"
)

type PublishRequest struct {
	PluginRequest    *pub.ReadRequest
	OpSession        *OpSession
	RealTimeSettings *RealTimeSettings
	Store            store.BoltStore
	Data             meta.RowMap
	Meta             meta.RowMap
	// The schema info for the shape in the request
	Schema  *meta.Schema
	Cause   *Cause
	Action  pub.Record_Action
	Record  *pub.Record
	Changes []Changes
	// Map from SchemaID of the dependency table to a RecordData containing the keys/values
	// of the row the current data depends on.
	Dependencies  map[string]meta.RowMap
	RealTimeState *RealTimeState
}

func (p PublishRequest) WithChanges(c []Changes) PublishRequest {
	p.Changes = c
	return p
}

type Changes struct {
	DependencyID string
	// Each item in this slice is a map containing
	// - The key values for the schema, keyed by opaque name
	// - The key values for the dependency table, keyed by opaque name
	Data []meta.RowMap
}

type PublishHandler interface {
	Handle(req PublishRequest) error
}

type PublishHandlerFunc func(item PublishRequest) error

func (f PublishHandlerFunc) Handle(item PublishRequest) error {
	return f(item)
}

type PublishMiddleware func(handler PublishHandler) PublishHandler

func ApplyMiddleware(handler PublishHandler, middlewares ...PublishMiddleware) PublishHandler {
	for _, middleware := range middlewares {
		name := DescribeMiddleware(middleware)
		mh := middleware(handler)
		handler = PublishHandlerFunc(func(item PublishRequest) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = errors.Errorf("%s: panic: %s\n%s", name, r, debug.Stack())

				}
			}()

			err = mh.Handle(item)
			if err != nil {
				return errors.Wrap(err, name)
			}
			return nil
		})
	}
	return handler
}

func PublishToStreamHandler(stream pub.Publisher_PublishStreamServer) PublishHandler {
	mu := new(sync.Mutex)
	return PublishHandlerFunc(func(req PublishRequest) error {
		if req.OpSession.Ctx.Err() != nil {
			return nil
		}
		if req.Record == nil {
			return errors.Errorf("PublishToStreamHandler: record was not set on item %+v", req)
		}
		mu.Lock()
		defer mu.Unlock()
		return stream.Send(req.Record)
	})
}

func DescribeMiddlewares(middlewares ...PublishMiddleware) []string {
	names := make([]string, len(middlewares))
	for i, m := range middlewares {
		name := DescribeMiddleware(m)
		names[len(names)-i-1] = name
	}
	return names
}

func DescribeMiddleware(middleware PublishMiddleware) string {
	name := runtime.FuncForPC(reflect.ValueOf(middleware).Pointer()).Name()
	re := regexp.MustCompile(`naveego.*\.(.*).func\d*$`)
	matches := re.FindStringSubmatch(name)
	if len(matches) > 0 {
		return matches[1]
	}
	return "unknown"
}

type RecordCollector struct {
	Records []*pub.Record
	Items   []PublishRequest
}

func (r *RecordCollector) Handle(item PublishRequest) error {
	r.Items = append(r.Items, item)
	r.Records = append(r.Records, item.Record)
	return nil
}

func PublishToChannelHandler(out chan<- *pub.Record) PublishHandler {
	return PublishHandlerFunc(func(item PublishRequest) error {
		select {
		case <-item.OpSession.Ctx.Done():
		case out <- item.Record:
		}
		return nil
	})
}

func SetRecordMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {
		return PublishHandlerFunc(func(item PublishRequest) error {
			if item.Record == nil && item.Data != nil {
				item.Record = &pub.Record{
					DataJson: item.Data.JSON(),
					Action:   item.Action,
					Cause:    item.Cause.String(),
				}
			}
			return handler.Handle(item)
		})
	}
}

func GetRecordsStaticMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {

		return PublishHandlerFunc(func(req PublishRequest) error {
			var query string
			var err error
			originalReq := req.PluginRequest
			session := req.OpSession

			templateArgs, err := buildSchemaDataQueryArgs(req, nil)
			if err != nil {
				return errors.Wrap(err, "build schema query")
			}

			query, err = templates.RenderSchemaDataQuery(templateArgs)
			if err != nil {
				return errors.Errorf("could not build query: %v", err)
			}

			if originalReq.Limit > 0 {
				query = fmt.Sprintf("select top(%d) * from (%s) as q", originalReq.Limit, query)
			}

			session.Log.Debug("Prepared query for reading records.", "query", query)
			stmt, err := session.DB.Prepare(query)
			if err != nil {
				return errors.Errorf("error preparing query: %s\nquery text:\n%s", query, err)
			}
			defer stmt.Close()

			rows, err := stmt.Query()
			if err != nil {
				return errors.Errorf("error executing query: %s\nquery text:\n%s", err, query)
			}

			return handleRows(req, templateArgs, rows, nil, handler)
		})
	}
}

func InitializeRealTimeComponentsMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {
		return PublishHandlerFunc(func(req PublishRequest) error {

			var err error
			session := req.OpSession
			log := session.Log.Named("InitializeRealTimeComponentsMiddleware")
			log.Debug("Handler starting.")

			requestedSchema := req.PluginRequest.Schema
			var realTimeSettings RealTimeSettings
			var realTimeState RealTimeState
			err = json.Unmarshal([]byte(req.PluginRequest.RealTimeSettingsJson), &realTimeSettings)
			if err != nil {
				return errors.Wrap(err, "invalid real time settings")
			}
			if req.PluginRequest.RealTimeStateJson != "" {
				err = json.Unmarshal([]byte(req.PluginRequest.RealTimeStateJson), &realTimeState)
				if err != nil {
					return errors.Wrap(err, "invalid real time state")
				}
			}

			req.RealTimeSettings = &realTimeSettings
			req.RealTimeState = &realTimeState

			log.With("real_time_state", req.RealTimeState, "raw", req.PluginRequest.RealTimeStateJson).Info("Initialized with real time state.")

			if len(realTimeSettings.Tables) == 0 {
				schema := session.SchemaInfo[requestedSchema.Id]
				if schema == nil || !schema.IsTable {
					return errors.Errorf("schema `%s` is not a table, but no real time table settings are configured", requestedSchema.Id)
				}
				query, err := templates.RenderSelfBridgeQuery(templates.SelfBridgeQueryArgs{
					SchemaInfo: schema})
				if err != nil {

					return errors.Errorf("error rendering self dependency query for table %q: %s", schema.ID, err)
				}

				realTimeSettings.Tables = []RealTimeTableSettings{
					{
						SchemaID: schema.ID,
						Query:    query,
					},
				}
			}

			dir, jobID, currentDataVersion := req.OpSession.Config.PermanentDirectory, req.PluginRequest.JobId, int(req.PluginRequest.DataVersion)
			storePath, err := getDBPath(dir, jobID, currentDataVersion)
			if err != nil {
				return errors.Errorf("could not create local storage directory for file %q: %s", storePath, err)
			}
			err = deleteOldData(dir, jobID, currentDataVersion)
			if err != nil {
				log.Error("Could not clean up old local storage records after version change.", "error", err)
			}

			boltStore, err := store.GetBoltStore(storePath)
			if err != nil {
				return errors.Wrap(err, "open store")
			}
			req.Store = boltStore
			defer store.ReleaseBoltStore(boltStore)


			return handler.Handle(req)

		})
	}
}

func GetRecordsRealTimeMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {
		return PublishHandlerFunc(func(req PublishRequest) error {

			var err error

			session := req.OpSession
			log := session.Log.Named("GetRecordsRealTimeMiddleware")
			realTimeState := req.RealTimeState
			shape := req.PluginRequest.Schema

			minVersion := realTimeState.Version
			maxVersion, err := getChangeTrackingVersion(session)
			if err != nil {
				return errors.Wrap(err, "get next change tracking version")
			}



			realTimeSettings := req.RealTimeSettings
			err = req.Store.DB.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte(shape.Id))
				if err != nil {
					return errors.Errorf("create bucket for %q: %s", shape.Id, err)
				}
				_, err = b.CreateBucketIfNotExists([]byte(shape.Id))
				if err != nil {
					return errors.Errorf("create dep bucket for %q: %s", shape.Id, err)
				}

				for _, dep := range realTimeSettings.Tables {
					_, err = b.CreateBucketIfNotExists([]byte(dep.SchemaID))
					if err != nil {
						return errors.Errorf("create dep bucket for %q: %s", dep.SchemaID, err)
					}
				}
				return nil
			})


			initializeNeeded := false
			if minVersion == 0 {
				log.Info("No change tracking history found, running initial load of entire table.")
				initializeNeeded = true
			} else {
				// We need to check whether the min version from the committed state
				// is still valid. It may have expired if this job was paused for a long time.
				// If the min version is no longer valid, we'll need to re-initialize the data.
				var trackedSchemaIDs []string
				for _, t := range realTimeSettings.Tables {
					trackedSchemaIDs = append(trackedSchemaIDs, t.SchemaID)
				}
				if len(trackedSchemaIDs) == 0 {
					// no tracked source tables, the schema itself is tracked:
					trackedSchemaIDs = []string{req.PluginRequest.Schema.Id}
				}

				for _, schemaID := range trackedSchemaIDs {
					versionValid, err := validateChangeTrackingVersion(session, schemaID, minVersion)
					if err != nil {
						return errors.Wrap(err, "validate version")
					}
					if !versionValid {
						log.Info("Last committed version is no longer valid, running initial load of entire table.")
						initializeNeeded = true
					}
				}
			}

			if initializeNeeded {
				// Run a straight load from the table, using the default query.

				templateArgs, err := buildSchemaDataQueryArgs(req, nil)
				if err != nil {
					return errors.Wrap(err, "build schema query")
				}

				query, err := templates.RenderSchemaDataQuery(templateArgs)
				if err != nil {
					return errors.Wrap(err, "build real time schema query")
				}

				log.Trace("Rendered real time schema query")
				log.Trace(query)

				rows, err := session.DB.QueryContext(session.Ctx, query)
				if err != nil {
					return errors.Errorf("query failed: %s\nquery text:\n%s", err, query)
				}

				err = handleRows(req, templateArgs, rows, nil, handler)

				if err != nil {
					return errors.Wrap(err, "initial load of table")
				}

				log.Info("Completed initial load, committing current version checkpoint", "version", maxVersion)
				// Commit the version we captured before the initial load.
				// Now we want subsequent publishes to begin at the
				// version we got before we did the full load,
				// because we know that we've already published
				// all the changes before that version.
				minVersion, err = commitVersionToHandler(req, maxVersion, handler)
				if err != nil {
					return errors.Wrap(err, "commit version")
				}
			}

			interval, _ := time.ParseDuration(realTimeSettings.PollingInterval)
			if interval == 0 {
				interval = 5 * time.Second
			}

			queries := map[string]*sql.Stmt{}
			queryTexts := map[string]string{}
			for _, table := range realTimeSettings.Tables {
				depSchema := session.SchemaInfo[table.SchemaID]
				args := templates.ChangeDetectionQueryArgs{
					BridgeQuery:               table.Query,
					SchemaArgs:                MetaSchemaFromShape(req.PluginRequest.Schema),
					DependencyArgs:            depSchema,
					ChangeKeyPrefix:           ChangeKeyPrefix,
					ChangeOperationColumnName: ChangeOperationColumnName,
				}
				queryText, err := templates.RenderChangeDetectionQuery(args)
				queryTexts[table.SchemaID] = queryText
				if err != nil {
					return errors.Errorf("error building query for %q: %s", table.SchemaID, err)
				}

				queries[table.SchemaID], err = session.DB.Prepare(queryText)
				if err != nil {
					return errors.Errorf("error preparing query for %q: %s\nquery text:\n%s", table.SchemaID, err, queryText)
				}
			}

			defer func() {
				// Clean up prepared queries.
				for _, q := range queries {
					_ = q.Close()
				}
			}()

			for {

				// Capture the version for this point in time. We'll publish
				// The publish will get everything that has changed between
				// the previous max version and the current version.
				maxVersion, err = getChangeTrackingVersion(session)
				if err != nil {
					return errors.Wrap(err, "get next change tracking version")
				}
				log.Info("Got max version for this batch of changes", "maxVersion", maxVersion)
				if maxVersion == minVersion {
					log.Info("Version has not changed since last poll", "version", maxVersion)
				} else {

					var allChanges []Changes

					for _, table := range realTimeSettings.Tables {

						tableLog := log.Named(fmt.Sprintf("GetRecordsRealTime(%s)", table.SchemaID))

						if session.Ctx.Err() != nil {
							log.Warn("Session context has been cancelled")
							return nil
						}

						query := queries[table.SchemaID]

						tableLog.Debug("Getting changes since last commit", "minVersion", minVersion, "maxVersion", maxVersion)
						tableLog.Trace(queryTexts[table.SchemaID])

						rows, err := query.QueryContext(session.Ctx, sql.Named("minVersion", minVersion), sql.Named("maxVersion", maxVersion))
						if err != nil {
							return errors.Errorf("getting changes for %q: %s\nquery text:\n%s", table.SchemaID, err, queryTexts[table.SchemaID])
						}

						changes := Changes{
							DependencyID: table.SchemaID,
						}

						for rows.Next() {
							data, err := scanToMap(rows)
							if err != nil {
								return errors.Errorf("scanning changes for %q: %s", table.SchemaID, err)
							}
							changes.Data = append(changes.Data, data)
						}

						if len(changes.Data) == 0 {
							log.Debug("No changes detected in table")
							continue
						}

						allChanges = append(allChanges, changes)

					}

					if len(allChanges) > 0 {
						log.Debug("Changes detected in dependencies", "dependencies", len(allChanges))

						err = handler.Handle(req.WithChanges(allChanges))

						if err != nil {
							return errors.Errorf("next handler couldn't handle changes: %s", err)
						}
					}

					log.Debug("Completed publish of recent changes, committing max version", "maxVersion", maxVersion)
					// commit the most recent version we captured so that if something goes wrong we'll start at that version next time
					minVersion, err = commitVersionToHandler(req, maxVersion, handler)

				}

				log.Debug("Waiting until interval elapses before checking for more changes", "interval", interval)
				// wait until interval elapses, then we'll loop again.
				select {
				case <-time.After(interval):
					log.Debug("Interval elapsed")
				case <-session.Ctx.Done():
					log.Info("Session canceled")
					return nil
				}
			}
		})
	}
}

func StoreChangeTrackingDataMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {
		return PublishHandlerFunc(func(req PublishRequest) error {

			log := req.OpSession.Log.Named("StoreChangeTrackingData")

			if req.Data != nil {

				err := req.Store.DB.Update(func(tx *bolt.Tx) error {

					txHelper := NewTxHelper(tx)

					schemaKeys, err := req.Data.ExtractRowKeys(req.Schema)
					if err != nil {
						return errors.Errorf("invalid data: %s", err)
					}

					deleted, _ := req.Meta[DeletedFlagColumnName].(bool)

					var rowValues meta.RowValues
					if deleted {

					} else {
						rowValues, err = req.Data.ExtractRowValues(req.Schema)
						if err != nil {
							return errors.Errorf("invalid data: %s", err)
						}
					}

					keyBytes, valueHash := schemaKeys.Marshal(), rowValues.Hash()
					log.Trace("Got hashes", "key", string(keyBytes), "value", fmt.Sprintf("%x", valueHash), "data", rowValues.String())

					result, err := txHelper.SetSchemaHash(req.Schema.ID, schemaKeys, rowValues)

					switch result {
					case StoreNoop:
						// We still propagate the record here because the plugin doesn't
						// know whether the record has successfully been captured by go-between, even if
						// the plugin has already sent this record to go-between in the past.
						log.Trace("record has not changed", "keys", schemaKeys)
						req.Action = pub.Record_UPSERT
					case StoreAdded:
						log.Trace("record has been inserted", "keys", schemaKeys)
						req.Action = pub.Record_INSERT
					case StoreChanged:
						log.Trace("record has been changed", "keys", schemaKeys)
						req.Action = pub.Record_UPDATE
					case StoreDeleted:
						log.Trace("record has been deleted", "keys", schemaKeys)
						req.Action = pub.Record_DELETE
					}

					for depID, data := range req.Dependencies {

						schema := req.OpSession.SchemaInfo[depID]
						depKeys, err := data.ExtractRowKeys(schema)
						if err != nil {
							return errors.Errorf("dependency on %q had invalid data: %s", depID, err)
						}

						if result == StoreDeleted {
							err = txHelper.UnregisterDependency(depID, req.Schema.ID, depKeys, schemaKeys)
						} else {
							err = txHelper.RegisterDependency(depID, req.Schema.ID, depKeys, schemaKeys)
						}

						if err != nil {
							return errors.Errorf("dependency on %q could not be registered: %s", depID, err)
						}
					}

					return handler.Handle(req)

				})

				return err

			}

			return handler.Handle(req)

		})
	}
}

func ProcessChangesMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {
		return PublishHandlerFunc(func(req PublishRequest) error {

			if len(req.Changes) == 0 {
				return handler.Handle(req)
			}
			session := req.OpSession
			log := session.Log.Named("ProcessChanges")

			schema := req.Schema

			// uniqueRowKeys maps the serialized version of the RowKeys to
			// the RowKeys themselves. This is needed because a RowKeys
			// is really a slice, and a slice can't be a key in a dictionary.
			// We collect all row keys here to handle cases where there is
			// not a one-to-one relationship between dependencies and schema
			// records, and cases where the same schema record is impacted
			// by multiple changes in the same change batch.
			uniqueRowKeys := map[string]meta.RowKeys{}
			causes := map[string]Cause{}

			err := req.Store.DB.View(func(tx *bolt.Tx) error {
				txHelper := NewTxHelper(tx)
				for _, changeSet := range req.Changes {
					for _, change := range changeSet.Data {
						// First pass we add all the keys which are associated
						// with the records AFTER the change.
						schemaRowKeys, err := change.ExtractRowKeys(schema)
						if err != nil {
							return errors.Errorf("invalid schema row keys in change data %v: %s", change, err)
						}

						action, _ := change[ChangeOperationColumnName].(string)
						keyString := schemaRowKeys.ToMapKey()
						uniqueRowKeys[keyString] = schemaRowKeys

						depSchema := session.SchemaInfo[changeSet.DependencyID]
						depRowKeys, err := change.ExtractPrefixedRowKeys(depSchema, ChangeKeyPrefix)

						cause := Cause{Table: changeSet.DependencyID, Action: action, DependencyKeys: depRowKeys}
						causes[keyString] = cause

						// Then get all the keys which were associated with the changed
						// dependency record BEFORE the change
						oldDependents, err := txHelper.GetDependentSchemaKeys(changeSet.DependencyID, schema.ID, depRowKeys)
						if err != nil {
							return errors.Errorf("could not get dependent schema keys for schema %s, dependency %s: %s", schema.ID, changeSet.DependencyID, err)
						}
						for _, schemaRowKeys := range oldDependents {
							keyString = schemaRowKeys.ToMapKey()
							uniqueRowKeys[keyString] = schemaRowKeys
							causes[keyString] = cause
						}
					}
				}
				return nil
			})
			if err != nil {
				return err
			}

			// Now get the unique row keys for all the schema rows
			var allRowKeys []meta.RowKeys
			for _, rowKeys := range uniqueRowKeys {
				if !rowKeys.HasNils() {
					allRowKeys = append(allRowKeys, rowKeys)
				}
			}

			log.Trace("Rendering template for schema data changes query", "changed rows", len(allRowKeys))

			templateArgs, err := buildSchemaDataQueryArgs(req, allRowKeys)
			if err != nil {
				return errors.Wrap(err, "build schema data changes template args")
			}

			query, err := templates.RenderSchemaDataQuery(templateArgs)
			if err != nil {
				return errors.Wrap(err, "render schema data changes query")
			}

			log.Trace("Rendered schema data changes query")
			log.Trace(query)

			rows, err := session.DB.QueryContext(session.Ctx, query)
			if err != nil {
				return errors.Errorf("query failed: %s\nquery text:\n%s", err, query)
			}

			return handleRows(req, templateArgs, rows, causes, handler)
		})
	}
}

func buildSchemaDataQueryArgs(req PublishRequest, allRowKeys []meta.RowKeys) (templates.SchemaDataQueryArgs, error) {
	var err error

	templateArgs := templates.SchemaDataQueryArgs{
		RowKeys:               allRowKeys,
		DeletedFlagColumnName: DeletedFlagColumnName,
	}
	templateArgs.SchemaArgs = MetaSchemaFromShape(req.PluginRequest.Schema)
	templateArgs.SchemaArgs.Query, err = buildQuery(req.PluginRequest)
	if err != nil {
		return templateArgs, errors.Wrap(err, "build schema query")
	}
	if req.RealTimeSettings != nil {
		for _, dep := range req.RealTimeSettings.Tables {
			depArgs := req.OpSession.SchemaInfo[dep.SchemaID]
			depArgs.Query = dep.Query
			templateArgs.DependencyTables = append(templateArgs.DependencyTables, depArgs)
		}
	}

	return templateArgs, nil
}

// commitVersion commits the version by writing out a state commit to the out channel.
// It will return early if the session is cancelled. It returns the version for clarity.
func commitVersionToHandler(req PublishRequest, version int, handler PublishHandler) (int, error) {
	state := RealTimeState{
		Version: version,
	}
	req.Record = &pub.Record{
		Action:            pub.Record_REAL_TIME_STATE_COMMIT,
		RealTimeStateJson: state.String(),
	}

	return version, handler.Handle(req)
}

func BuildHandlerAndRequest(session *OpSession, externalRequest *pub.ReadRequest, handler PublishHandler) (PublishHandler, PublishRequest, error) {

	middlewares := []PublishMiddleware{
		SetRecordMiddleware(),
	}

	if externalRequest.RealTimeSettingsJson == "" {
		middlewares = append(middlewares, GetRecordsStaticMiddleware())
	} else {
		middlewares = append(middlewares,
			StoreChangeTrackingDataMiddleware(),
			ProcessChangesMiddleware(),
			GetRecordsRealTimeMiddleware(),
			InitializeRealTimeComponentsMiddleware(),
		)
	}

	names := DescribeMiddlewares(middlewares...)
	session.Log.Info("Preparing handler with middlewares...", "names", names)

	handler = ApplyMiddleware(handler, middlewares...)

	req := PublishRequest{
		OpSession:     session,
		PluginRequest: externalRequest,
		Schema:        MetaSchemaFromShape(externalRequest.Schema),
	}

	return handler, req, nil
}

func getItemsAndHandle(session *OpSession, externalRequest *pub.ReadRequest, handler PublishHandler) error {

	var err error
	var query string
	req := PublishRequest{
		OpSession:     session,
		PluginRequest: externalRequest,
	}
	templateArgs, err := buildSchemaDataQueryArgs(req, nil)

	query, err = templates.RenderSchemaDataQuery(templateArgs)
	if err != nil {
		return errors.Errorf("could not build query: %v", err)
	}

	if externalRequest.Limit > 0 {
		query = fmt.Sprintf("select top(%d) * from (%s) as q", externalRequest.Limit, query)
	}

	stmt, err := session.DB.Prepare(query)
	if err != nil {
		return errors.Errorf("prepare query %q: %s", query, err)
	}
	session.Log.Debug("Prepared query for reading records.", "query", query)
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}

	return handleRows(req, templateArgs, rows, nil, handler)
}

func handleRows(req PublishRequest, args templates.SchemaDataQueryArgs, rows *sql.Rows, causes map[string]Cause, handler PublishHandler) error {

	var err error
	columns, err := rows.ColumnTypes()
	if err != nil {
		return errors.Errorf("error getting columns from result set: %s", err)
	}

	session := req.OpSession
	shape := req.PluginRequest.Schema

	shapePropertiesMap := map[string]*pub.Property{}
	for _, p := range shape.Properties {
		shapePropertiesMap[p.Name] = p
	}

	metaMap := map[string]bool{
		DeletedFlagColumnName: true,
	}

	dependencyColumnMap := map[string]*meta.Column{}
	for _, dep := range args.DependencyTables {
		for _, col := range dep.Columns() {
			dependencyColumnMap[col.OpaqueName()] = col
		}
	}

	valueBuffer := make([]interface{}, len(columns))
	dataBuffer := make(map[string]interface{}, len(columns))
	metaBuffer := make(map[string]interface{}, len(columns))

	for rows.Next() {
		if session.Ctx.Err() != nil {
			return nil
		}

		for i, c := range columns {
			if p, ok := shapePropertiesMap[c.Name()]; ok {
				// this column contains a property defined on the shape
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
			} else if ok := metaMap[c.Name()]; ok {
				// this column contains a meta property added to the query by us
				valueBuffer[i] = &valueBuffer[i]
			} else if _, ok := dependencyColumnMap[c.Name()]; ok {
				// this column contains a dependency column property
				valueBuffer[i] = &valueBuffer[i]
			}
		}
		err = rows.Scan(valueBuffer...)
		if err != nil {
			return errors.WithStack(err)
		}

		action := pub.Record_UPSERT

		for i, c := range columns {
			value := valueBuffer[i]
			if value != nil {
				r := reflect.ValueOf(value)
				for r.Kind() == reflect.Ptr {
					r = r.Elem()
					if r.IsValid() {
						value = r.Interface()
					}
				}
			}

			if p, ok := shapePropertiesMap[c.Name()]; ok {
				// this column contains a property defined on the shape
				dataBuffer[p.Id] = value
			} else if ok := metaMap[c.Name()]; ok {
				metaBuffer[c.Name()] = value
			} else if col, ok := dependencyColumnMap[c.Name()]; ok {
				if req.Dependencies == nil {
					req.Dependencies = map[string]meta.RowMap{}
				}
				entry, ok := req.Dependencies[col.SchemaID]
				if !ok {
					entry = meta.RowMap{}
					req.Dependencies[col.SchemaID] = entry
				}
				entry[col.ID] = value
			}
		}

		session.Log.Trace("Publishing record", "action", action, "data", dataBuffer)

		req.Data = dataBuffer
		req.Meta = metaBuffer

		rowKeys, _ := req.Data.ExtractRowKeys(req.Schema)
		if cause, ok := causes[rowKeys.ToMapKey()]; ok {
			req.Cause = &cause
		}

		err = handler.Handle(req)
		if err != nil {
			return err
		}
	}

	return err
}

// Scans the current row into a map and returns it.
// Assumes rows.Next() has already been called.
func scanToMap(rows *sql.Rows) (map[string]interface{}, error) {

	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Errorf("error getting columns from result set: %s", err)
	}

	valueBuffer := make([]interface{}, len(columns))
	dataBuffer := make(map[string]interface{}, len(columns))

	for i := range columns {
		valueBuffer[i] = &valueBuffer[i]
	}
	err = rows.Scan(valueBuffer...)
	if err != nil {
		return nil, errors.Wrap(err, "scan rows")
	}

	for i, c := range columns {
		value := valueBuffer[i]
		dataBuffer[c.Name()] = value
	}

	return dataBuffer, rows.Err()
}

// getDBPath gets the path for a DB for the specified scenario.
func getDBPath(dir string, jobID string, dataVersion int) (string, error) {
	dbDir := filepath.Join(dir, "plugin-pub-mssql", jobID)
	err := os.MkdirAll(dbDir, 0700)
	if err != nil {
		return "", err
	}

	return filepath.Join(dbDir, fmt.Sprint(dataVersion)), nil
}

func deleteOldData(dir string, jobID string, currentDataVersion int) error {
	dbDir := filepath.Join(dir, "plugin-pub-mssql", jobID)
	files, err := ioutil.ReadDir(dbDir)
	if err != nil {
		return err
	}
	current := fmt.Sprint(currentDataVersion)
	for _, file := range files {
		name := filepath.Base(file.Name())
		if name != current {
			err := os.Remove(filepath.Join(dbDir, file.Name()))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
