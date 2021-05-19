package sqlstructs

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/dimdin/decimal"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"reflect"
	"strconv"
	"strings"
)

// Insert inserts the struct into the named table.
func Insert(db *sql.DB, table string, item interface{}) error {

	itemValue := reflect.ValueOf(item)
	itemType := itemValue.Type()

	columnNames := make([]string, 0, itemValue.NumField())
	parameterNames := make([]string, 0, itemValue.NumField())
	parameters := make([]interface{}, 0, itemValue.NumField())

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
		parameterNames = append(parameterNames, "@"+parameterName)
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

	columnTypes, err := rows.ColumnTypes()
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

					switch v := val.(type) {
					case []uint8:
						numstr := string(v)
						val, err = strconv.ParseFloat(numstr, 64)
						if err != nil {
							return errors.Wrapf(err, "could not parse value %v into a float", v)
						}
					}

					rval := reflect.ValueOf(val)
					switch rval.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						rval = rval.Convert(f.Type)
					}
					dest := outElement.Elem().FieldByName(f.Name)

					if !rval.Type().AssignableTo(dest.Type()) {
						columnType := columnTypes[i]
						return errors.Errorf("could not assign value %s (%T) (sql type %s) to column %s (%s)", val, val, columnType.Name(), name, dest.Type())
					}

					dest.Set(rval)
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
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var out []map[string]interface{}

	for rows.Next() {
		columns := make([]interface{}, len(columnNames))
		columnPointers := make([]interface{}, len(columnNames))

		outElement := map[string]interface{}{}

		for i := 0; i < len(columnNames); i++ {

			columnType := columnTypes[i]
			switch getCanonicalSQLType(columnType) {
			case "DECIMAL", "MONEY", "SMALLMONEY":
				var d *decimal.Dec
				columnPointers[i] = &d
				break
			case "UNIQUEIDENTIFIER":
				var d *[]byte
				columnPointers[i] = &d
				break
			default:
				columnPointers[i] = &columns[i]

			}
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, errors.WithStack(err)
		}

		for i, name := range columnNames {
			columnType := columnTypes[i]
			// fmt.Printf("%d: %s: %#v %s\n", i, name, columnType, columnType.ScanType().Name())
			v := columnPointers[i]
			if v == nil {
				outElement[name] = nil
			} else {
				outElement[name] = reflect.ValueOf(v).Elem().Interface()
			}
			switch getCanonicalSQLType(columnType) {
			case "DECIMAL", "MONEY", "SMALLMONEY":
				if !reflect.ValueOf(outElement[name]).IsNil() {
					outElement[name] = fmt.Sprint(outElement[name])
				}
				break
			case "BINARY", "VARBINARY":
				value := outElement[name]
				if b, ok := (value).([]byte); ok {
					outElement[name] = base64.StdEncoding.EncodeToString(b)
				}

			case "UNIQUEIDENTIFIER":
				value := outElement[name]
				if bp, ok := (value).(*[]byte); ok && bp != nil {
					b := *bp
					// SQL package mangles the guid bytes, this fixes it
						b[0], b[1], b[2], b[3] = b[3], b[2], b[1], b[0]
						b[4], b[5] = b[5], b[4]
						b[6], b[7] = b[7], b[6]
					parsed, parseErr := uuid.FromBytes(b)
					if parseErr == nil {
						outElement[name] = parsed.String()
					}
				}
			}

		}
		out = append(out, outElement)
	}

	return out, nil
}

func getCanonicalSQLType(columnType *sql.ColumnType) string {
	return strings.ToUpper(strings.Split(columnType.DatabaseTypeName(), "(")[0])
}
