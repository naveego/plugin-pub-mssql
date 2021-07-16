package internal_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/constants"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
)

var _ = Describe("Replication Writeback", func() {

	var (
		session *Session
		op      *OpSession
		ctx     context.Context
		cancel  func()
		req     *pub.PrepareWriteRequest
		replicationSettings ReplicationSettings
	)

	BeforeEach(func() {

		Expect(db.Exec(`IF OBJECT_ID('w3.Replication.Golden', 'U') IS NOT NULL
    DROP TABLE w3.[Replication].Golden;

IF OBJECT_ID('w3.Replication.Versions', 'U') IS NOT NULL
    DROP TABLE w3.[Replication].Versions;`)).To(Not(BeNil()))

		log := GetLogger()
		session = &Session{
			Log: log,
			SchemaDiscoverer: SchemaDiscoverer{
				Log: log,
			},
			Ctx: context.Background(),
		}
		settings := GetTestSettings()

		replicationSettings = ReplicationSettings{
			SQLSchema:          "Replication",
			GoldenRecordTable:  "Golden",
			VersionRecordTable: "Versions",
		}

		connectionString, err := settings.GetConnectionString()

		session.Settings = settings

		session.DB, err = sql.Open("sqlserver", connectionString)
		Expect(err).ToNot(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
		op = session.OpSession(ctx)

		req = &pub.PrepareWriteRequest{
			Schema: &pub.Schema{
				Id: "TEST",
				Name: "TestShape",
				Properties: []*pub.Property{
					{
						Id:   "id",
						Name: "ID",
						Type: pub.PropertyType_INTEGER,
					},
					{
						Id:   "first_name",
						Name: "First Name",
						Type: pub.PropertyType_STRING,
					}, {
						Id:   "last_name",
						Name: "Last Name",
						Type: pub.PropertyType_STRING,
					}, {
						Id:   "dob",
						Name: "Date of Birth",
						Type: pub.PropertyType_DATE,
					},
					{
						Id:   "latitude",
						Name: "Latitude",
						Type: pub.PropertyType_DECIMAL,
					},
				},
			},
			Replication: &pub.ReplicationWriteRequest{
				SettingsJson: replicationSettings.JSON(),
				Versions: []*pub.ReplicationWriteVersion{
					{
						JobId:          "job-1",
						JobName:        "job-1-name",
						ConnectionId:   "connection-1",
						ConnectionName: "connection-1-name",
						SchemaId:       "schema-1",
						SchemaName:     "schema-1-name",
					}, {
						JobId:          "job-2",
						JobName:        "job-2-name",
						ConnectionId:   "connection-2",
						ConnectionName: "connection-2-name",
						SchemaId:       "schema-2",
						SchemaName:     "schema-2-name",
					},
				},
			},
		}

	})

	AfterEach(func() {
		if cancel != nil {
			cancel()
		}
	})

	It("should create tables when writer is initialized", func() {

		_, err := NewReplicationWriteHandler(op, req)
		Expect(err).ToNot(HaveOccurred())

		schemas, err := DiscoverSchemasSync(op, session.SchemaDiscoverer, &pub.DiscoverSchemasRequest{
			Mode:       pub.DiscoverSchemasRequest_ALL,
			SampleSize: 0,
		})

		var golden *pub.Schema
		var versions *pub.Schema
		for _, schema := range schemas {
			if schema.Id == "[Replication].[Golden]" {
				golden = schema
			}
			if schema.Id == "[Replication].[Versions]" {
				versions = schema
			}
		}
		Expect(golden).ToNot(BeNil())
		Expect(versions).ToNot(BeNil())
	})

	It("should write data to replication", func() {

		const pathPrefix = "testdata/replication_basics/"
		records := GetInputs(pathPrefix, req)

		sut, err := NewReplicationWriteHandler(op, req)

		Expect(err).ToNot(HaveOccurred())

		for _, record := range records {
			Expect(sut.Write(op, record)).To(Succeed())
		}

		goldenExpectations, versionExpectations := GetExpectations(pathPrefix)
		Expect(goldenExpectations).ToNot(BeEmpty())
		Expect(versionExpectations).ToNot(BeEmpty())
		goldenActuals, versionActuals := GetActuals(op, replicationSettings)
		Expect(goldenActuals).ToNot(BeEmpty())
		Expect(versionActuals).ToNot(BeEmpty())

		for _, expectation := range goldenExpectations {
			Expect(goldenActuals).To(ContainElement(BeEquivalentTo(expectation)), "in golden records")
		}
		for _, expectation := range versionExpectations {
			Expect(versionActuals).To(ContainElement(BeEquivalentTo(expectation)), "in version records")
		}

		Expect(goldenActuals).To(HaveLen(len(goldenExpectations)), "only the expected golden records should be present")
		Expect(versionActuals).To(HaveLen(len(versionExpectations)), "only the expected version records should be present")


	})

	It("should write data to replication and treat all null records as deletes", func() {

		const pathPrefix = "testdata/replication_deletes/"
		records := GetInputs(pathPrefix, req)

		sut, err := NewReplicationWriteHandler(op, req)

		Expect(err).ToNot(HaveOccurred())

		for _, record := range records {
			Expect(sut.Write(op, record)).To(Succeed())
		}

		goldenExpectations, versionExpectations := GetExpectations(pathPrefix)
		Expect(goldenExpectations).ToNot(BeEmpty())
		Expect(versionExpectations).ToNot(BeEmpty())
		goldenActuals, versionActuals := GetActuals(op, replicationSettings)
		Expect(goldenActuals).ToNot(BeEmpty())
		Expect(versionActuals).ToNot(BeEmpty())

		for _, expectation := range goldenExpectations {
			Expect(goldenActuals).To(ContainElement(BeEquivalentTo(expectation)), "in golden records")
		}
		for _, expectation := range versionExpectations {
			Expect(versionActuals).To(ContainElement(BeEquivalentTo(expectation)), "in version records")
		}

		Expect(goldenActuals).To(HaveLen(len(goldenExpectations)), "only the expected golden records should be present")
		Expect(versionActuals).To(HaveLen(len(versionExpectations)), "only the expected version records should be present")

	})

})

func GetInputs(pathPrefix string, req *pub.PrepareWriteRequest) []*pub.UnmarshalledRecord {

	path := pathPrefix + "input.json"
	b, err := ioutil.ReadFile(path)
	Expect(err).ToNot(HaveOccurred())

	var records []*pub.UnmarshalledRecord
	err = json.Unmarshal(b, &records)
	Expect(err).ToNot(HaveOccurred())

	for _, record := range records {
		Expect(len(record.UnmarshalledVersions)).To(BeNumerically("<=", len(req.Replication.Versions)))

		for i := range record.UnmarshalledVersions {
			vr := record.UnmarshalledVersions[i]
			v := req.Replication.Versions[i]

			vr.SchemaId = v.SchemaId
			vr.ConnectionId = v.ConnectionId
			vr.JobId = v.JobId
		}
	}

	return records
}

func GetActuals(session *OpSession, settings ReplicationSettings) (golden []map[string]interface{}, versions []map[string]interface{}) {

	goldenRows, err := session.DB.Query(fmt.Sprintf(`select * from [%s].[%s]`, settings.SQLSchema, settings.GoldenRecordTable))
	Expect(err).ToNot(HaveOccurred())

	golden, err = sqlstructs.UnmarshalRowsToMaps(goldenRows)
	Expect(err).ToNot(HaveOccurred())

	for _, item := range golden{
		delete(item, constants.CreatedAt)
		delete(item, constants.UpdatedAt)
	}

	// re-type everything to match the expectations types
	j, _ := json.Marshal(golden)
	_ = json.Unmarshal(j, &golden)

	versionRows, err := session.DB.Query(fmt.Sprintf(`select * from [%s].[%s]`, settings.SQLSchema, settings.VersionRecordTable))
	Expect(err).ToNot(HaveOccurred())

	versions, err = sqlstructs.UnmarshalRowsToMaps(versionRows)
	Expect(err).ToNot(HaveOccurred())

	for _, item := range versions{
		delete(item, constants.CreatedAt)
		delete(item, constants.UpdatedAt)
	}

	// re-type everything to match the expectations types
	j, _ = json.Marshal(versions)
	_ = json.Unmarshal(j, &versions)

	return

}

func GetExpectations(pathPrefix string) (golden []map[string]interface{}, versions []map[string]interface{}) {

	groupPath := pathPrefix + "expectations_groups.json"
	b, err := ioutil.ReadFile(groupPath)
	Expect(err).ToNot(HaveOccurred())

	Expect(json.Unmarshal(b, &golden)).To(Succeed())


	versionPath := pathPrefix + "expectations_versions.json"
	b, err = ioutil.ReadFile(versionPath)
	Expect(err).ToNot(HaveOccurred())

	Expect(json.Unmarshal(b, &versions)).To(Succeed())

	return
}

func rekeyMap(m map[string]interface{}, keys map[string]string) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range m {
		out[keys[k]] = v
	}
	return out

}
