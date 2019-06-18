package internal_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	. "github.com/naveego/plugin-pub-mssql/internal"
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

		Expect(db.Exec(`IF OBJECT_ID(' w3.ReplicationTest.Golden', 'U') IS NOT NULL
    DROP TABLE w3.ReplicationTest.Golden;

IF OBJECT_ID(' w3.ReplicationTest.Versions', 'U') IS NOT NULL
    DROP TABLE w3.ReplicationTest.Versions;`)).To(Not(BeNil()))

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
			SQLSchema:          "ReplicationTest",
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
			if schema.Id == "[ReplicationTest].[Golden]" {
				golden = schema
			}
			if schema.Id == "[ReplicationTest].[Versions]" {
				versions = schema
			}
		}
		Expect(golden).ToNot(BeNil())
		Expect(versions).ToNot(BeNil())
	})

	FIt("should write data to replication", func() {

		records := GetUnmarshalledReplicationRecords("testdata/unmarshalled_replication.json", req)

		sut, err := NewReplicationWriteHandler(op, req)

		Expect(err).ToNot(HaveOccurred())

		for _, record := range records {
			Expect(sut.Write(op, record)).To(Succeed())
		}

		goldenExpectations, versionExpectations := GetExpectations(req, records, sut.(*ReplicationWriter))
		Expect(goldenExpectations).ToNot(BeEmpty())
		Expect(versionExpectations).ToNot(BeEmpty())
		goldenActuals, versionActuals := GetActuals(op, replicationSettings)
		Expect(goldenActuals).ToNot(BeEmpty())
		Expect(versionActuals).ToNot(BeEmpty())

		Expect(goldenActuals).To(ConsistOf(goldenExpectations))
		Expect(versionActuals).To(ConsistOf(versionExpectations))

	})

})

func GetUnmarshalledReplicationRecords(path string, req *pub.PrepareWriteRequest) []*pub.UnmarshalledRecord {

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

	goldenRows, err := session.DB.Query(fmt.Sprintf(`select * from %s.%s`, settings.SQLSchema, settings.GoldenRecordTable))
	Expect(err).ToNot(HaveOccurred())

	golden, err = sqlstructs.UnmarshalRowsToMaps(goldenRows)
	Expect(err).ToNot(HaveOccurred())

	versionRows, err := session.DB.Query(fmt.Sprintf(`select * from %s.%s`, settings.SQLSchema, settings.VersionRecordTable))
	Expect(err).ToNot(HaveOccurred())

	versions, err = sqlstructs.UnmarshalRowsToMaps(versionRows)
	Expect(err).ToNot(HaveOccurred())


	return

}

func GetExpectations(req *pub.PrepareWriteRequest, inputs []*pub.UnmarshalledRecord, writer *ReplicationWriter) (golden []map[string]interface{}, versions []map[string]interface{}) {

	jobs := map[string]string{}
	connections := map[string]string{}
	schemas := map[string]string{}
	for _, v := range req.Replication.Versions {
		jobs[v.JobId] = v.JobName
		connections[v.ConnectionId] = v.ConnectionName
		schemas[v.SchemaId] = v.SchemaName
	}

	for _, input := range inputs {
		g := rekeyMap(input.Data, writer.GoldenIDMap)
		g["GroupID"] = input.RecordId
		golden = append(golden, g)

		for _, version := range input.UnmarshalledVersions {
			v := rekeyMap(version.Data, writer.VersionIDMap)
			v["JobName"] = jobs[version.JobId]
			v["ConnectionName"] = connections[version.ConnectionId]
			v["SchemaName"] = schemas[version.SchemaId]
			v["GroupID"] = input.RecordId
			v["RecordID"] = version.RecordId
			versions = append(versions, v)
		}
	}
	return
}

func rekeyMap(m map[string]interface{}, keys map[string]string) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range m {
		out[keys[k]] = v
	}
	return out

}
