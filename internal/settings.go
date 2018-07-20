package internal

import (
	"errors"
	"fmt"
)

type Settings struct {
	Server string `json:"server"`
	Database string `json:"database"`
	Auth AuthType `json:"auth"`
	Username string `json:"username"`
	Password string `json:"password"`
	PrePublishQuery string `json:"prePublishQuery"`
	PostPublishQuery string `json:"postPublishQuery"`
}

type AuthType string
const (
	AuthTypeSQL = AuthType("sql")
	AuthTypeWindows = AuthType("windows")
)


// Validate returns an error if the Settings are not valid.
// It also populates the internal fields of settings.
func (s *Settings) Validate() error {
	if s.Server == "" {
		return errors.New("the server property must be set")
	}

	if s.Database == "" {
		return errors.New("the database property must be set")
	}

	switch s.Auth{
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