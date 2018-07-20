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
	"log"
	"time"
)

var db *sql.DB

func TestCsv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MSSQL Suite")
}

var _ = BeforeSuite(func(){
	var err error

	Eventually(connectToSQL, time.Second * 60, time.Second).Should(Succeed())

	testDataPath := os.ExpandEnv("$GOPATH/src/github.com/naveego/plugin-pub-mssql/test/test_data.sql")
	testDataBytes, err := ioutil.ReadFile(testDataPath)
	Expect(err).ToNot(HaveOccurred())

	cmdText := string(testDataBytes)

	cmds := strings.Split(cmdText, "GO;")

	for _, cmd := range cmds {
		Expect(db.Exec(cmd)).ToNot(BeNil(), "should execute command " + cmd)
	}
})

func connectToSQL() error {
	var err error
	connectionString := "sqlserver://sa:n5o_ADMIN@localhost:1433"

	db, err = sql.Open("sqlserver", connectionString)
	if err != nil {
		log.Printf("Error connecting to SQL Server: %s", err)
		return err
	}
	err = db.Ping()
	if err != nil {
		log.Printf("Error pinging SQL Server: %s", err)
		return err
	}

	_, err = db.Exec(`IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'w3')
	BEGIN
	CREATE DATABASE w3
	END`)
	if err != nil {
		log.Printf("Error ensuring that w3 database exists: %s", err)
		return err
	}

	connectionString += "?database=w3"
	db, _ = sql.Open("sqlserver", connectionString)
	err = db.Ping()
	if err != nil {
		log.Printf("Error pinging w3 database: %s", err)
		return err
	}

	return err
}

var _ = AfterSuite(func(){
	db.Close()
})
