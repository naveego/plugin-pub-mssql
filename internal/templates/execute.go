package templates

import (
	"database/sql"
	"github.com/pkg/errors"
)

type Renderer interface{
	Render() (string, error)
}

func ExecuteCommand(db *sql.DB, renderer Renderer) (sql.Result, error) {
	command, err := renderer.Render()
	if err != nil {
		return nil, errors.Wrapf(err, "render failed with args %v", renderer)
	}
	// ensure metadata table exists
	result, err := db.Exec(command)
	if err != nil {
		return nil, errors.Wrapf(err, "execute failed for sql command\n%s", command)
	}

	return result, err
}