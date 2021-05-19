package internal

import (
	"encoding/json"
	"fmt"
	"github.com/naveego/go-json-schema"
	"github.com/pkg/errors"
	"net/url"
)

// Settings object for plugin
// Contains connection information and pre/post queries
type Settings struct {
	Host             string   `json:"host"`
	Port             int      `json:"port"`
	Instance         string   `json:"instance"`
	Database         string   `json:"database"`
	Auth             AuthType `json:"auth"`
	Username         string   `json:"username"`
	Password         string   `json:"password"`
	AdvancedSettings string   `json:"advancedSettings"`
	PrePublishQuery  string   `json:"prePublishQuery"`
	PostPublishQuery string   `json:"postPublishQuery"`
	SkipCustomQueryCount bool `json:"skipCustomQueryCount"`
}

// AuthType underlying type
type AuthType string

// Authentication types
const (
	AuthTypeSQL     = AuthType("sql")
	AuthTypeWindows = AuthType("windows")
)

// Validate returns an error if the Settings are not valid.
// It also populates the internal fields of settings.
func (s *Settings) Validate() error {
	if s.Host == "" {
		return errors.New("the host property must be set")
	}

	if s.Database == "" {
		return errors.New("the database property must be set")
	}

	switch s.Auth {
	case AuthTypeSQL:
		if s.Username == "" {
			return errors.New("when auth type is 'sql' the username property must be set")
		}
		if s.Password == "" {
			return errors.New("when auth type is 'sql' the password property must be set")
		}
	case AuthTypeWindows:
	case "":
		return errors.New("the auth property must be set")
	default:
		return fmt.Errorf("unrecognized auth type %q", s.Auth)
	}

	return nil
}

// GetConnectionString builds a connection string from a settings object
func (s *Settings) GetConnectionString() (string, error) {
	var host string
	err := s.Validate()
	if err != nil {
		return "", err
	}

	if s.Port != 0 {
		host = fmt.Sprintf("%s:%d", s.Host, s.Port)
	} else {
		host = fmt.Sprintf("%s:%d", s.Host, 1433)
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		Host:     host,
		Path:     s.Instance, // if connecting to an instance instead of a port
		RawQuery: fmt.Sprintf("database=%s", s.Database),
	}

	switch s.Auth {
	case AuthTypeSQL:
		u.User = url.UserPassword(s.Username, s.Password)
	}

	return fmt.Sprintf("%s;%s", u.String(), s.AdvancedSettings), nil
}

type ErrorMap map[string]interface{}

const ErrorMapKey = "__errors"

func (e ErrorMap) AddError(err string, args ...interface{}) {
	errs, _ := e[ErrorMapKey].([]string)
	errs = append(errs, fmt.Sprintf(err, args...))
	e[ErrorMapKey] = errs
}

func (e ErrorMap) GetOrAddChild(key interface{}) ErrorMap {
	keyAsString := fmt.Sprint(key)
	child, ok := e[keyAsString]
	if !ok {
		child = ErrorMap{}
		e[keyAsString] = child
	}
	switch x := child.(type) {
	case map[string]interface{}: // after unmarshalling
		return ErrorMap(x)
	case ErrorMap:
		return x
	default:
		panic(errors.Errorf("unexpected type %T at key %q", x, keyAsString))
	}
}

func (e ErrorMap) GetErrors(path ...string) []string {
	switch len(path) {
	case 0:
		errs, _ := e[ErrorMapKey]
		switch x := errs.(type){
		case []string:
			return x

		case []interface{}: // after unmarshalling
		var out []string
		for _, i := range x {
			s, _ := i.(string)
			out = append(out, s)
		}
		return out
		case nil:
			return nil
		default:
			panic(errors.Errorf("unexpected type %T at error key %q", x, ErrorMapKey))
		}
	default:
		return e.GetOrAddChild(path[0]).GetErrors(path[1:]...)
	}
}

func (e ErrorMap) String() string {
	j, _  := json.Marshal(e)
	return string(j)
}

type SchemaMap map[string]interface{}

func (s SchemaMap) String() string {
	b, _ := json.MarshalIndent(s, "", "  ")
	return string(b)
}

type RealTimeState struct {
	Version int `json:"version"`
	Versions map[string]int `json:"versions"`
}

func (r RealTimeState) String() string{
	b, _ := json.MarshalIndent(r, "", "  ")
	return string(b)
}

type RealTimeTableState struct {
	Version int `json:"version"`
}

type RealTimeSettings struct {
	meta string `title:"Real Time Settings" description:"Configure the tables to monitor for changes."`
	Tables []RealTimeTableSettings `json:"tables" title:"Tables" description:"Add tables which will be checked for changes." minLength:"1"`
	PollingInterval string `json:"pollingInterval" title:"Polling Interval" default:"5s" description:"Interval between checking for changes.  Defaults to 5s." pattern:"\\d+(ms|s|m|h)" required:"true"`
}

func (r RealTimeSettings) String() string {
	b, _ := json.MarshalIndent(r, "", "  ")
	return string(b)
}

type RealTimeTableSettings struct {
	CustomTarget 		   string `json:"customTarget" title:"Custom Target" description:"Custom target for change tracking."`
	SchemaID               string `json:"schemaID" title:"Table" description:"The table to monitor for changes." required:"true"`
	Query                  string `json:"query"  required:"true" title:"Query" description:"A query which matches up the primary keys of the the table where change tracking is enabled with the keys of the view or query you are publishing from." `
}

func GetRealTimeSchemas() (form *jsonschema.JSONSchema, ui SchemaMap) {

	form = jsonschema.NewGenerator().WithRoot(RealTimeSettings{}).MustGenerate()


	_ = updateProperty(&form.Property, func(p *jsonschema.Property) {
		p.Pattern = `\d+(ms|s|m|h)`
	}, "pollingInterval")

	_ = updateProperty(&form.Property, func(p *jsonschema.Property) {
		var min int64 = 1
		p.MinLength = &min
	}, "tables")

	ui = SchemaMap{
		"pollingInterval": SchemaMap {
			"ui:help": "Provide a number and a unit (s = second, m = minute, h = hour).",
		},
		"tables": SchemaMap{
			"items": SchemaMap{
				"ui:order": []string{"schemaID", "customTarget", "query"},
				"query":SchemaMap{
					"ui:widget":"textarea",
					"ui:options": SchemaMap{
						"rows":3,
					},
					"ui:help": "The query must select the keys into columns with specific names." +
						"The columns from the change tracked table must be named `[Dependency.{column}]`," +
						"where `{column}` is the name of the column. The columns from the view or query " +
						"must be named `[Schema.{column}]`",
				},
				"customTarget":SchemaMap{
					"ui:help": "Format: `[database].[schema].[table]`",
				},
			},
		},
	}

	return
}

func GetMapFromJSONSchema(js *jsonschema.JSONSchema) SchemaMap {
	b, _ := json.Marshal(js)
	var out SchemaMap
	json.Unmarshal(b, &out)
	return out
}

func setInMap(m map[string]interface{}, value interface{}, path ...string) error {
	if m == nil {
		return errors.New("nil map")
	}
	if len(path) == 0 {
		return errors.New("ran out of path")
	}

	key := path[0]

	if len(path) == 1 {
		m[key] = value
		return nil
	}

	var child map[string]interface{}
	c, ok := m[key]
	if !ok {
		child = map[string]interface{}{}
		m[key] = child
	} else {
		child, ok = c.(map[string]interface{})
		if !ok {
			return errors.Errorf("%s: non-map value %s (%T) at path", key, c, c)
		}
	}

	err := setInMap(child, value, path[1:]...)
	if err != nil {
		return errors.Errorf("%s.%s", key, err)
	}

	return nil
}

func updateProperty(property *jsonschema.Property, fn func(property *jsonschema.Property), path ...string) error {
	if property == nil {
		return errors.New("no entry")
	}

	if len(path) == 0 {
		fn(property)
		return nil
	}

	if property.Type == "array"{
		return updateProperty(property.Items, fn, path...)
	}

	if property.Properties == nil{
		property.Properties = map[string]*jsonschema.Property{}
	}

	child, ok := property.Properties[path[0]]
	if !ok {
		child = &jsonschema.Property{
			Type:"object",
		}
		property.Properties[path[0]] = child
	}


	err := updateProperty(child, fn, path[1:]...)
	if err != nil {
		return errors.Errorf("%s.%s", path[0], err)
	}

	return nil
}