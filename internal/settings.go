package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/naveego/go-json-schema"
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
	PrePublishQuery  string   `json:"prePublishQuery"`
	PostPublishQuery string   `json:"postPublishQuery"`
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

	return u.String(), nil
}

type SchemaMap map[string]interface{}

func (s SchemaMap) String() string {
	b, _ := json.MarshalIndent(s, "", "  ")
	return string(b)
}

type RealTimeState struct {
	Tables []RealTimeTableState `json:"tables"`
}

type RealTimeTableState struct {
	Offset int `json:"offset"`
}

type RealTimeSettings struct {
	Tables []RealTimeSettings `json:"tables"`
}

type RealTimeTableSettings struct {
	TableName              string `json:"tableName"`
	TranslateKeyUsingQuery bool   `json:"translateKeyUsingQuery"`
	Query                  string `json:"query,omitempty"`
}

func GetRealTimeSchemas() (form *jsonschema.JSONSchema, ui SchemaMap) {

	form = jsonschema.NewGenerator().WithRoot(RealTimeSettings{}).MustGenerate()

	ui = SchemaMap{
		"ui:order": []string{"tableName", "translateKeyUsingQuery", "query"},
	}

	return
}

func getMapFromJSONSchema(js jsonschema.JSONSchema) SchemaMap {
	b, _ := json.Marshal(js)
	var out SchemaMap
	json.Unmarshal(b, &out)
	return out
}
