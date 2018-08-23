// Copyright © 2017 Naveego

package main

import (
	"github.com/naveego/plugin-pub-mssql/cmd"
	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	cmd.Execute()
}
