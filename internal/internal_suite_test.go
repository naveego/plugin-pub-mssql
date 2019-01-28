package internal_test

import (
	"github.com/as/hue"
	"github.com/hashicorp/go-hclog"
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
	log hclog.Logger
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
		Level:      hclog.Trace,
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

	cmds := splitScriptRE.Split(cmdText, -1)

	for _, cmd := range cmds {
		Expect(db.Exec(cmd)).ToNot(BeNil(), "should execute command "+cmd)
	}

	Expect(connectToSQL("w3")).To(Succeed())

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
