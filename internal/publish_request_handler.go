package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/internal/templates"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	"github.com/naveego/plugin-pub-mssql/pkg/store"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
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

func PublishToStreamHandler(ctx context.Context, stream pub.Publisher_PublishStreamServer) PublishHandler {
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
		done := make(chan error)
		go func() {
			done <- stream.Send(req.Record)
		}()
		t := time.NewTimer(5 * time.Minute)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-done:
			t.Stop()
			return err
		case <-t.C:
			return errors.Errorf("stream.Send timed out; agent may be too busy to handle our data")
		}
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

			hasKeys := false
			for _, p := range requestedSchema.Properties {
				if p.IsKey {
					hasKeys = true
					break
				}
			}
			if !hasKeys {
				return errors.Errorf("the schema %q (%q) has no keys defined", requestedSchema.Name, requestedSchema.Id)
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

			realTimeSettings := req.RealTimeSettings
			session := req.OpSession
			log := session.Log.Named("GetRecordsRealTimeMiddleware")
			realTimeState := req.RealTimeState
			shape := req.PluginRequest.Schema

			var versionedTableIDs []string
			for _, t := range realTimeSettings.Tables {
				versionedTableIDs = append(versionedTableIDs, getTableSchemaId(t))
			}
			if len(versionedTableIDs) == 0 {
				// no tracked source tables, the schema itself is tracked:
				versionedTableIDs = []string{req.PluginRequest.Schema.Id}
			}

			minVersions := realTimeState.Versions
			if realTimeState.Version != 0 {
				minVersions = VersionSet{}
				// migrate from previous storage
				for _, tableID := range versionedTableIDs {
					minVersions[tableID] = realTimeState.Version
				}
			}

			maxVersions, err := getChangeTrackingVersion(session, versionedTableIDs)
			if err != nil {
				return errors.Wrap(err, "get next change tracking version")
			}

			log.Debug("Got request", "req", req)
			log.Debug("Got real time settings", "settings", realTimeSettings)

			err = req.Store.DB.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte(shape.Id))
				if err != nil {
					return errors.Errorf("create bucket for %q: %s", shape.Id, err)
				}

				for _, dep := range realTimeSettings.Tables {
					_, err = b.CreateBucketIfNotExists([]byte(getTableSchemaId(dep)))
					if err != nil {
						return errors.Errorf("create dep bucket for %q: %s", getTableSchemaId(dep), err)
					}
				}
				return nil
			})

			initializeNeeded := false
			if (minVersions == nil || len(minVersions) == 0) &&  realTimeState.Version == 0 {
				initializeNeeded = true
			} else {
				// We need to check whether the min version from the committed state
				// is still valid. It may have expired if this job was paused for a long time.
				// If the min version is no longer valid, we'll need to re-initialize the data.
				var trackedSchemaIDs []string
				for _, t := range realTimeSettings.Tables {
					trackedSchemaIDs = append(trackedSchemaIDs, getTableSchemaId(t))
				}
				if len(trackedSchemaIDs) == 0 {
					// no tracked source tables, the schema itself is tracked:
					trackedSchemaIDs = []string{req.PluginRequest.Schema.Id}
				}

				for _, schemaID := range trackedSchemaIDs {
					versionValid, err := validateChangeTrackingVersion(session, schemaID, minVersions[schemaID])
					if err != nil {
						return errors.Wrap(err, "validate version")
					}
					if !versionValid {
						log.Debug("Last committed version is no longer valid, running initial load of entire table.")
						initializeNeeded = true
					}
				}
			}

			if initializeNeeded {
				// Run a straight load from the table, using the default query.

				templateArgs, err := buildSchemaDataQueryArgs(req, nil)
				if err != nil {
					log.Error("Error building query", "error", err.Error())
					return errors.Wrap(err, "build schema query")
				}
				log.Debug("buildSchemaDataQueryArgs completed")

				query, err := templates.RenderSchemaDataQuery(templateArgs)
				if err != nil {
					log.Error("Error render data query", "error", err.Error())
					return errors.Wrap(err, "build real time schema query")
				}

				log.Debug("Rendered real time schema query")
				log.Debug(query)

				rows, err := session.DB.QueryContext(session.Ctx, query)
				if err != nil {
					log.Error("Error query context", "error", err.Error())
					return errors.Errorf("real time schema query failed: %s\nquery text:\n%s", err, query)
				}

				err = handleRows(req, templateArgs, rows, nil, handler)

				if err != nil {
					log.Error("Error handle rows", "error", err.Error())
					return errors.Wrap(err, "initial load of table")
				}

				log.Debug("Completed initial load, committing current version checkpoint", "version", maxVersions)
				// Commit the version we captured before the initial load.
				// Now we want subsequent publishes to begin at the
				// version we got before we did the full load,
				// because we know that we've already published
				// all the changes before that version.
				minVersions, err = commitVersionToHandler(req, maxVersions, handler)
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
				depSchema := session.SchemaInfo[getTableSchemaId(table)]
				if depSchema == nil {
					depSchema, err = getMetaSchemaForSchemaId(session, getTableSchemaId(table))
					if err != nil {
						return errors.Errorf("error getting args for building query for %q: %s", getTableSchemaId(table), err)
					}
				}
				args := templates.ChangeDetectionQueryArgs{
					BridgeQuery:               table.Query,
					SchemaArgs:                MetaSchemaFromPubSchema(req.PluginRequest.Schema),
					DependencyArgs:            depSchema,
					ChangeKeyPrefix:           ChangeKeyPrefix,
					ChangeOperationColumnName: ChangeOperationColumnName,
				}
				queryText, err := templates.RenderChangeDetectionQuery(args)
				queryTexts[getTableSchemaId(table)] = queryText
				if err != nil {
					return errors.Errorf("error building query for %q: %s", getTableSchemaId(table), err)
				}

				queries[getTableSchemaId(table)], err = session.DB.Prepare(queryText)
				if err != nil {
					return errors.Errorf("error preparing query for %q: %s\nquery text:\n%s", getTableSchemaId(table), err, queryText)
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
				maxVersions, err = getChangeTrackingVersion(session, versionedTableIDs)
				if err != nil {
					return errors.Wrap(err, "get next change tracking version")
				}
				log.Debug("Got max version for this batch of changes", "maxVersion", maxVersions)

				var allChanges []Changes

				for _, table := range realTimeSettings.Tables {
					if maxVersions[getTableSchemaId(table)] == minVersions[getTableSchemaId(table)] {
						log.Debug("Version has not changed since last poll", "version", maxVersions)
					} else {
						tableLog := log.Named(fmt.Sprintf("GetRecordsRealTime(%s)", getTableSchemaId(table)))

						if session.Ctx.Err() != nil {
							log.Warn("Session context has been cancelled")
							return nil
						}

						query := queries[getTableSchemaId(table)]

						tableLog.Debug("Getting changes since last commit", "minVersion", minVersions, "maxVersion", maxVersions)
						tableLog.Trace(queryTexts[getTableSchemaId(table)])

						rows, err := query.QueryContext(session.Ctx, sql.Named("minVersion", minVersions[getTableSchemaId(table)]), sql.Named("maxVersion", maxVersions[getTableSchemaId(table)]))
						if err != nil {
							return errors.Errorf("getting changes for %q: %s\nquery text:\n%s", getTableSchemaId(table), err, queryTexts[getTableSchemaId(table)])
						}

						changes := Changes{
							DependencyID: getTableSchemaId(table),
						}

						rawChangeData, err := sqlstructs.UnmarshalRowsToMaps(rows)
						if err != nil {
							return errors.Errorf("scanning changes for %q: %s", getTableSchemaId(table), err)
						}
						changes.Data = make([]meta.RowMap, len(rawChangeData))
						for i, rawChangeRow := range rawChangeData {
							changes.Data[i] = rawChangeRow
						}

						// for rows.Next() {
						// 	data, err := scanToMap(rows)
						//
						// 	changes.Data = append(changes.Data, data)
						// }

						if len(changes.Data) == 0 {
							log.Debug("No changes detected in table", "table name", getTableSchemaId(table))
							continue
						}

						log.Debug("Changes detected in table", "table name", getTableSchemaId(table), "total changes", len(changes.Data))

						allChanges = append(allChanges, changes)
					}
				}

				if len(allChanges) > 0 {
					log.Debug("Changes detected in dependencies", "total dependencies", len(allChanges))

					err = handler.Handle(req.WithChanges(allChanges))

					if err != nil {
						return errors.Errorf("next handler couldn't handle changes: %s", err)
					}

					log.Debug("Completed publish of recent changes, committing max version", "maxVersion", maxVersions)
					// commit the most recent version we captured so that if something goes wrong we'll start at that version next time
					minVersions, err = commitVersionToHandler(req, maxVersions, handler)
				} else {
					log.Debug("No changes detected in dependencies")
				}


				log.Debug("Waiting until interval elapses before checking for more changes", "interval", interval)
				// wait until interval elapses, then we'll loop again.
				select {
				case <-time.After(interval):
					log.Debug("Interval elapsed")
				case <-session.Ctx.Done():
					log.Debug("Session canceled")
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
						if schema == nil {
							schema, err = getMetaSchemaForSchemaId(req.OpSession, depID)
							if err != nil {
								return errors.Errorf("error getting meta schema for %q: %s", depID, err)
							}
						}

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

			const maxChangeBatchSize = float64(999)

			log.Trace("Processing detected changes", "changed rows", len(allRowKeys))

			for len(allRowKeys) > 0 {
				// if there are more row keys than fit in an insert,
				// we need to do multiple queries

				changeBatchSize := int(math.Min(maxChangeBatchSize, float64(len(allRowKeys))))

				batchRowKeys := allRowKeys[0:changeBatchSize]
				allRowKeys = allRowKeys[changeBatchSize:]

				templateArgs, err := buildSchemaDataQueryArgs(req, batchRowKeys)
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

				err = handleRows(req, templateArgs, rows, causes, handler)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}
}

func buildSchemaDataQueryArgs(req PublishRequest, allRowKeys []meta.RowKeys) (templates.SchemaDataQueryArgs, error) {
	var err error

	templateArgs := templates.SchemaDataQueryArgs{
		RowKeys:               allRowKeys,
		DeletedFlagColumnName: DeletedFlagColumnName,
	}
	templateArgs.SchemaArgs = MetaSchemaFromPubSchema(req.PluginRequest.Schema)
	templateArgs.SchemaArgs.Query, err = buildQuery(req.PluginRequest)
	if err != nil {
		return templateArgs, errors.Wrap(err, "build schema query")
	}
	if req.RealTimeSettings != nil {
		for _, dep := range req.RealTimeSettings.Tables {
			depArgs := req.OpSession.SchemaInfo[getTableSchemaId(dep)]

			if depArgs == nil {
				depArgs, err = getMetaSchemaForSchemaId(req.OpSession, getTableSchemaId(dep))
				if err != nil {
					return templateArgs, errors.Wrap(err, "get meta schema")
				}
			}

			depArgs.Query = dep.Query
			templateArgs.DependencyTables = append(templateArgs.DependencyTables, depArgs)
		}
	}

	return templateArgs, nil
}

// commitVersion commits the version by writing out a state commit to the out channel.
// It will return early if the session is cancelled. It returns the version for clarity.
func commitVersionToHandler(req PublishRequest, version VersionSet, handler PublishHandler) (VersionSet, error) {
	state := RealTimeState{
		Versions: version,
	}
	req.Record = &pub.Record{
		Action:            pub.Record_REAL_TIME_STATE_COMMIT,
		RealTimeStateJson: state.String(),
	}

	return version, handler.Handle(req)
}

type VersionSet map[string]int

func (v VersionSet) String() string {
	j, _ := json.Marshal(v)
	return string(j)
}

func (v VersionSet) Equals(other VersionSet) bool {
	for k, vs := range v {
		if other[k] != vs {
			return false
		}
	}
	for k, vs := range other {
		if v[k] != vs {
			return false
		}
	}
	return true
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
	session.Log.Debug("Preparing handler with middlewares...", "names", names)

	handler = ApplyMiddleware(handler, middlewares...)

	req := PublishRequest{
		OpSession:     session,
		PluginRequest: externalRequest,
		Schema:        MetaSchemaFromPubSchema(externalRequest.Schema),
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

	session := req.OpSession
	shape := req.PluginRequest.Schema

	var err error
	columns, err := rows.ColumnTypes()
	if err != nil {
		return errors.Errorf("error getting columns from result set: %s", err)
	}

	session.Log.Debug("handleRows Got columns")

	shapePropertiesMap := map[string]*pub.Property{}
	for _, p := range shape.Properties {
		shapePropertiesMap[p.Id] = p
	}

	session.Log.Debug("handleRows made shape prop map")

	metaMap := map[string]bool{
		DeletedFlagColumnName: true,
	}

	dependencyColumnMap := map[string]*meta.Column{}
	for _, dep := range args.DependencyTables {
		for _, col := range dep.Columns() {
			dependencyColumnMap[col.OpaqueName()] = col
		}
	}

	session.Log.Debug("handleRows made dependency column map")

	valueBuffer := make([]interface{}, len(columns))
	dataBuffer := make(map[string]interface{}, len(columns))
	metaBuffer := make(map[string]interface{}, len(columns))

	escapedNameMap := make(map[string]string, len(columns))
	for _, c := range columns {
		escapedNameMap[c.Name()] = "[" + c.Name() + "]"
	}

	session.Log.Debug("handleRows made escaped name map")

	for rows.Next() {
		if session.Ctx.Err() != nil {
			return nil
		}

		for i, c := range columns {
			if p, ok := shapePropertiesMap[escapedNameMap[c.Name()]]; ok {
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

		session.Log.Debug("handleRows scanned row into buffer")

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

			// Handle parsing to correct type:
			switch strings.ToUpper(c.DatabaseTypeName()) {
			case "UNIQUEIDENTIFIER":
				if b, ok := (value).([]byte); ok {
					// SQL package mangles the guid bytes, this fixes it
					b[0], b[1], b[2], b[3] = b[3], b[2], b[1], b[0]
					b[4], b[5] = b[5], b[4]
					b[6], b[7] = b[7], b[6]
					parsed, parseErr := uuid.FromBytes(b)
					if parseErr == nil {
						value = strings.ToUpper(parsed.String())
					}
				}
				break
			}

			if p, ok := shapePropertiesMap[escapedNameMap[c.Name()]]; ok {
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
