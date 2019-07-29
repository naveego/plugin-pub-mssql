package internal_test

import (
	"fmt"
	"github.com/as/hue"
	"github.com/hashicorp/go-hclog"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"database/sql"
	"io/ioutil"

	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/ci/go/build"
	. "github.com/naveego/plugin-pub-mssql/internal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var db *sql.DB

var (
	allSchemaIDs   []string
	tableSchemaIDs []string
	viewSchemaIDs  []string
	testOutput     io.Writer
	log            hclog.Logger
)

func TestMSSQL(t *testing.T) {
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

	reOut := hue.NewRegexpWriter(GinkgoWriter)
	reOut.AddRuleStringPOSIX(hue.New(hue.Magenta, hue.Default), `\[TRACE\]`)
	reOut.AddRuleStringPOSIX(hue.New(hue.Green, hue.Default), `\[DEBUG\]`)
	reOut.AddRuleStringPOSIX(hue.New(hue.Blue, hue.Default), `\[INFO \]`)
	reOut.AddRuleStringPOSIX(hue.New(hue.Brown, hue.Default), `\[WARN \]`)
	reOut.AddRuleStringPOSIX(hue.New(hue.Red, hue.Default), `\[ERROR\]`)

	testOutput = reOut

	log = hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Debug,
		Output:     testOutput,
		JSONFormat: false,
	})

	envTimeout, ok := os.LookupEnv("TEST_TIMEOUT")
	if ok {
		if envTimeoutDuration, err := time.ParseDuration(envTimeout); err == nil {
			SetDefaultEventuallyTimeout(envTimeoutDuration)
		}
	}

	Eventually(func() error { return connectToSQL("master") }, 60*time.Second, time.Second).Should(Succeed())

	_, thisPath, _, _ := runtime.Caller(0)
	testDataPath := filepath.Join(thisPath, "../../test/test_data.sql")
	testDataBytes, err := ioutil.ReadFile(testDataPath)
	Expect(err).ToNot(HaveOccurred())

	manifestFilePath := filepath.Join(thisPath, "../../manifest.json")
	manifest, err := ioutil.ReadFile(manifestFilePath)
	Expect(err).ToNot(HaveOccurred())
	Expect(ioutil.WriteFile("manifest.json", manifest, 0700)).To(Succeed())

	cmdText := string(testDataBytes)

	Expect(db.Exec(`IF NOT EXISTS(SELECT name FROM master.dbo.sysdatabases WHERE name = N'w3')
  BEGIN
    CREATE DATABASE w3

    ALTER DATABASE [w3] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS)
  END`)).ToNot(BeNil())

	Expect(connectToSQL("w3")).To(Succeed())

	cmds := splitScriptRE.Split(cmdText, -1)

	log.Info("Preparing database.")
	for _, cmd := range cmds {
		log.Trace(cmd)
		Expect(db.Exec(cmd)).ToNot(BeNil(), "should execute command "+cmd)
	}

	rows, err := db.Query(`SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE
FROM w3.INFORMATION_SCHEMA.TABLES
ORDER BY TABLE_NAME`)

	Expect(err).ToNot(HaveOccurred())

	for rows.Next() {
		var schema, name, typ string
		Expect(rows.Scan(&schema, &name, &typ)).To(Succeed())
		id := GetSchemaID(schema, name)
		allSchemaIDs = append(allSchemaIDs, id)
		if typ == "BASE TABLE" {
			tableSchemaIDs = append(tableSchemaIDs, id)
		} else {
			viewSchemaIDs = append(viewSchemaIDs, id)
		}
	}
})

var splitScriptRE = regexp.MustCompile(`(?m:^GO;?\s*$)`)

var _ = AfterSuite(func() {
	db.Close()
	os.Remove("manifest.json")
})

func connectToSQL(database string) error {
	var err error
	var connectionString string
	settings := GetTestSettings()

	// initially set Database to master to validate or create test db w3
	settings.Database = database

	connectionString, err = settings.GetConnectionString()
	if err != nil {
		return err
	}

	db, err = sql.Open("sqlserver", connectionString)
	if err != nil {
		log.Error("Error connecting to SQL Server", "error", err)
		return err
	}
	err = db.Ping()
	if err != nil {
		log.Error("Error pinging SQL Server", "error", err)
		return err
	}

	return err
}


func GetLogger() hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		Output:     testOutput,
		JSONFormat: false,
	})
}

func HaveOccurredStack() types.GomegaMatcher{
	return &HaveOccurredStackMatcher{}
}

type HaveOccurredStackMatcher struct {
}

func (matcher *HaveOccurredStackMatcher) Match(actual interface{}) (success bool, err error) {
	// is purely nil?
	if actual == nil {
		return false, nil
	}

	// must be an 'error' type
	if _, ok := actual.(error); !ok {
		return false, fmt.Errorf("Expected an error-type.  Got:\n%s", format.Object(actual, 1))
	}

	// must be non-nil (or a pointer to a non-nil)
	return true, nil
}

func (matcher *HaveOccurredStackMatcher) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected an error to have occurred.  Got:\n%+v", actual)
}

func (matcher *HaveOccurredStackMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected error:\n%+v\n%s", actual, "not to have occurred")
}