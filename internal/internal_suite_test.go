package internal_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	_ "github.com/denisenkom/go-mssqldb"
	"database/sql"
	"io/ioutil"
	"os"
	"strings"
)

var db *sql.DB

func TestCsv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MSSQL Suite")
}

var _ = BeforeSuite(func(){
	var err error
	connectionString := "sqlserver://sa:n5o_ADMIN@127.0.0.1"
	db, err = sql.Open("sqlserver", connectionString)
	Expect(err).ToNot(HaveOccurred())

	testDataPath := os.ExpandEnv("$GOPATH/src/github.com/naveego/plugin-pub-mssql/test/test_data.sql")
	testDataBytes, err := ioutil.ReadFile(testDataPath)
	Expect(err).ToNot(HaveOccurred())

	cmdText := string(testDataBytes)

	cmds := strings.Split(cmdText, "GO;")

	for _, cmd := range cmds {
		Expect(db.Exec(cmd)).ToNot(BeNil(), "should execute command " + cmd)
	}



})

var _ = AfterSuite(func(){
	db.Close()
})
