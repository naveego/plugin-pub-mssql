package sqlstructs

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"strings"
)

// Insert inserts the struct into the named table.
func Insert(db *sql.DB, table string, item interface{}) error {

	itemValue := reflect.ValueOf(item)
	itemType := itemValue.Type()


	columnNames := make([]string,0, itemValue.NumField())
	parameterNames := make([]string,0, itemValue.NumField())
	parameters := make([]interface{},0, itemValue.NumField())

	for i := 0; i < itemValue.NumField(); i++ {
		f := itemType.Field(i)
		if _, ok := f.Tag.Lookup("sqlkey"); ok {
			continue
		}
		n := f.Name
		if t, ok := f.Tag.Lookup("sql"); ok {
			n = t
		}
		columnNames = append(columnNames, n)
		parameterName := fmt.Sprintf("p%d", i)
		parameterNames = append(parameterNames, "@" + parameterName)
		parameters = append(parameters, sql.Named(parameterName, itemValue.Field(i).Interface()))
	}

	query := fmt.Sprintf(`insert into %s (%s) 
values (%s)`,
table,
strings.Join(columnNames, ", "),
strings.Join(parameterNames, ", "))

	_, err := db.Exec(query, parameters...)
	if err != nil {
		return errors.Wrapf(err, "insert attempt with command %q", query)
	}

	return err
}

// The out parameter must be a pointer to a slice of the type each
// row should be unmarshaled into.
func UnmarshalRows(rows *sql.Rows, out interface{}) error {

	columnNames, err := rows.Columns()
	if err != nil {
		return errors.WithStack(err)
	}

	outValue := reflect.ValueOf(out)
	if outValue.Kind() != reflect.Ptr {
		return errors.Errorf("out must be pointer, was %s", outValue.Type())
	}

	outSlice := outValue.Elem()
	if outSlice.Kind() != reflect.Slice {
		return errors.Errorf("out must be pointer to slice, was %s", outValue.Type())
	}

	outElementType := outSlice.Type().Elem()
	outElementTypeIsPointer := outElementType.Kind() == reflect.Ptr
	if outElementTypeIsPointer {
		outElementType = outElementType.Elem()
	}

	if outElementType.Kind() != reflect.Struct {
		return errors.Errorf("out must be pointer to slice of structs, was %s", outValue.Type())
	}

	outElementFields := map[string]reflect.StructField{}

	for i := 0; i < outElementType.NumField(); i++ {
		f := outElementType.Field(i)
		n := f.Name
		if t, ok := f.Tag.Lookup("sql"); ok {
			n = t
		}
		outElementFields[n] = f
	}

	for rows.Next() {
		columns := make([]interface{}, len(columnNames))
		columnPointers := make([]interface{}, len(columnNames))

		outElement := reflect.New(outElementType)

		for i := 0; i < len(columnNames); i++ {
			n := columnNames[i]
			if f, ok := outElementFields[n]; ok {
				columns[i] = reflect.New(f.Type).Interface()
			}

			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return errors.WithStack(err)
		}

		for i, name := range columnNames {
			if f, ok := outElementFields[name]; ok {
				val := columns[i]
				if val != nil {

					rval := reflect.ValueOf(val)
					switch rval.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						rval = rval.Convert(f.Type)
					}

					outElement.Elem().FieldByName(f.Name).Set(rval)
				}
			}
		}

		if !outElementTypeIsPointer {
			outElement = outElement.Elem()
		}

		outSlice = reflect.Append(outSlice, outElement)
	}

	outValue.Elem().Set(outSlice)

	return nil
}

func UnmarshalRowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var out []map[string]interface{}

	for rows.Next() {
		columns := make([]interface{}, len(columnNames))
		columnPointers := make([]interface{}, len(columnNames))

		outElement := map[string]interface{}{}

		for i := 0; i < len(columnNames); i++ {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, errors.WithStack(err)
		}

		for i, name := range columnNames {
			v := columnPointers[i]
			if v == nil {
			outElement[name] = nil
			} else {
				outElement[name] = reflect.ValueOf(v).Elem().Interface()
			}

		}
		out = append(out, outElement)
	}

	return out, nil
}