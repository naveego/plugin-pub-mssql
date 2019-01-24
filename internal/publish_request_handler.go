package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
)

type PublishItem struct {
	OpSession *OpSession
	Data      RecordData
	Meta      RecordData
	Cause     Cause
	Record    *pub.Record
}

type RecordData map[string]interface{}

func (s RecordData) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type ItemHandler interface{
	Handle(item *PublishItem) error
}

type ItemHandlerFunc func(item *PublishItem) error

func (f ItemHandlerFunc) Handle(item *PublishItem) error {
	return f(item)
}

type ItemMiddleware func(handler ItemHandler) ItemHandler

func ApplyMiddleware(handler ItemHandler, middlewares ...ItemMiddleware) ItemHandler {
	for _, middleware := range middlewares {
		handler = middleware(handler)
	}
	return handler
}

func PublishToStreamHandler(stream pub.Publisher_PublishStreamServer) ItemHandler {
	return labelErrors("PublishToStream", ItemHandlerFunc(func(item *PublishItem) error {
		if item.OpSession.Ctx.Err() != nil{
			return nil
		}
		if item.Record == nil {
			return errors.Errorf("record was not set on item %+v", item)
		}

		return stream.Send(item.Record)
	}))
}

type RecordCollector struct {
	Records []*pub.Record
	Items []*PublishItem
}

func (r *RecordCollector) Handle(item *PublishItem) error {
	r.Items = append(r.Items, item)
	r.Records = append(r.Records, item.Record)
	return nil
}


func PublishToChannelHandler(out chan<- *pub.Record) ItemHandler {
	return labelErrors("PublishToChannel", ItemHandlerFunc(func(item *PublishItem) error {
		select {
		case <-item.OpSession.Ctx.Done():
		case out <- item.Record:
		}
		return nil
	}))
}

func SetRecordMiddleware() ItemMiddleware {
	return func(handler ItemHandler) ItemHandler {
		return labelErrors("SetRecord", ItemHandlerFunc(func(item *PublishItem) error {
			item.Record = &pub.Record{
				DataJson: item.Data.String(),
				Action:   pub.Record_UPSERT,
				Cause:    item.Cause.String(),
			}
			return handler.Handle(item)
		}))
	}
}

func labelErrors(label string, handler ItemHandler) ItemHandler {
	return ItemHandlerFunc(func(item *PublishItem) error {
		err := handler.Handle(item)
		if err != nil {
			return errors.Wrap(err, label)
		}
		return nil
	})
}

func getItemsAndHandle(session *OpSession, req *pub.PublishRequest, handler ItemHandler) error {

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

	rows, err := stmt.Query()
	if err != nil {
		return err
	}

	return handleRows(session, req.Shape, rows, handler)
}

func readRecordsUsingQuery(session *OpSession, shape *pub.Shape, out chan<- *pub.Record, stmt *sql.Stmt, args ...interface{}) error {
	// rows, err := stmt.Query(args...)
	// if err != nil {
	// 	return err
	// }
	//
	// parsedRows := make(chan ParsedRow)
	//
	// go func() {
	// 	defer close(parsedRows)
	// 	for {
	// 		select {
	// 		case <-session.Ctx.Done():
	// 			return
	// 		case row := <-parsedRows:
	// 			out <- &pub.Record{
	// 				DataJson: row.Data.String(),
	// 				Action:   pub.Record_UPSERT,
	// 				Cause:    row.Cause.String(),
	// 			}
	// 		}
	// 	}
	// }()
	//
	// return parseRows(session, shape, rows, parsedRows)
	return nil
}

func handleRows(session *OpSession, shape *pub.Shape, rows *sql.Rows, handler ItemHandler) error {

	var err error
	columns, err := rows.ColumnTypes()
	if err != nil {
		return errors.Errorf("error getting columns from result set: %s", err)
	}

	shapePropertiesMap := map[string]*pub.Property{}
	for _, p := range shape.Properties {
		shapePropertiesMap[p.Name] = p
	}

	metaMap := map[string]bool{
		naveegoActionHint: true,
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
			}
		}
		err = rows.Scan(valueBuffer...)
		if err != nil {
			return errors.WithStack(err)
		}

		action := pub.Record_UPSERT

		for i, c := range columns {
			value := valueBuffer[i]

			if p, ok := shapePropertiesMap[c.Name()]; ok {
				// this column contains a property defined on the shape
				dataBuffer[p.Id] = value
			} else if ok := metaMap[c.Name()]; ok {
				metaBuffer[c.Name()] = value
			}
		}

		session.Log.Trace("Publishing record", "action", action, "data", dataBuffer)

		item := &PublishItem{
			Data:      dataBuffer,
			Meta:      metaBuffer,
			OpSession: session,
		}

		err = handler.Handle(item)
		if err != nil {
			return err
		}
	}

	return err
}
