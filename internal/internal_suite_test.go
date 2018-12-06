package internal_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"database/sql"
	"io/ioutil"
	"log"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/ci/go/build"
	. "github.com/naveego/plugin-pub-mssql/internal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var db *sql.DB

func TestCsv(t *testing.T) {
	RegisterFailHandler(Fail)
	build.RunSpecsWithReporting(t, "MSSQL Suite")
}

func GetTestSettings() *Settings {
	return &Settings{
		Host:     "localhost",
		Port:     1433,
		Auth:     AuthTypeSQL,
		Username: "sa",
		Password: "n5o_ADMIN",
		Database: "w3",
	}
}

var _ = BeforeSuite(func() {
	var err error

	Eventually(connectToSQL, 60*time.Second, time.Second).Should(Succeed())

	_, thisPath, _, _ := runtime.Caller(0)
	testDataPath := filepath.Join(thisPath, "../../test/test_data.sql")
	testDataBytes, err := ioutil.ReadFile(testDataPath)
	Expect(err).ToNot(HaveOccurred())

	cmdText := string(testDataBytes)

	cmds := strings.Split(cmdText, "GO;")

	for _, cmd := range cmds {
		Expect(db.Exec(cmd)).ToNot(BeNil(), "should execute command "+cmd)
	}
})

func connectToSQL() error {
	var err error
	var connectionString string
	settings := GetTestSettings()

	// initially set Database to master to validate or create test db w3
	settings.Database = "master"

	connectionString, err = settings.GetConnectionString()
	if err != nil {
		return err
	}

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

	// change db context to w3
	settings.Database = "w3"

	connectionString, err = settings.GetConnectionString()
	if err != nil {
		return err
	}

	db, _ = sql.Open("sqlserver", connectionString)
	err = db.Ping()
	if err != nil {
		log.Printf("Error pinging w3 database: %s", err)
		return err
	}

	return err
}

var _ = AfterSuite(func() {
	db.Close()
})
