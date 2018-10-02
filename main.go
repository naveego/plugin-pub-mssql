// Copyright © 2017 Naveego

package main

import (
	"github.com/naveego/plugin-pub-mssql/cmd"
	_ "github.com/denisenkom/go-mssqldb"
	// imported to prevent dep from evicting it, dep doesn't scan magefile.go
	_ 	"github.com/naveego/ci/go/build"

)

func main() {
	cmd.Execute()
}
