package sqlstructs

import (
	"database/sql"
	"github.com/pkg/errors"
	"reflect"
)

// UnmarshalRows unmarshals the data in rows into the out parameter.
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

				outElement.Elem().FieldByName(f.Name).Set(reflect.ValueOf(val))
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