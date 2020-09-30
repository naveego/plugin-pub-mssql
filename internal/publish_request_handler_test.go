package internal_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"context"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega/types"
)

func testMiddleware() PublishMiddleware {
	return func(handler PublishHandler) PublishHandler {
		return PublishHandlerFunc(func(item PublishRequest) error {
			return handler.Handle(item)
		})
	}
}

var _ = Describe("PublishRequestHandler helpers", func() {
	It("should label errors as expected", func() {
		handler := PublishHandlerFunc(func(item PublishRequest) error {
			return errors.New("expected")
		})
		sut := ApplyMiddleware(handler, testMiddleware())

		err := sut.Handle(PublishRequest{})
		Expect(err).To(HaveOccurred())
		msg := err.Error()
		Expect(msg).To(ContainSubstring("expected"))
		Expect(msg).To(ContainSubstring("testMiddleware:"))
	})
})

type DeveloperRecord struct {
	ID   int    `sql:"id" json:"[id]"`
	Name string `sql:"name" json:"[name]"`
}

type RealTimeRecord struct {
	ID          int    `sql:"id" json:"[id]"`
	OwnValue    string `sql:"ownValue" json:"[ownValue]"`
	MergeValue  string `sql:"mergeValue" json:"[mergeValue]"`
	SpreadValue string `sql:"spreadValue" json:"[spreadValue]"`
}

type RealTimeDuplicateViewRecord struct {
	RecordID    int    `sql:"recordID" json:"[recordID]"`
	OwnValue    string `sql:"ownValue" json:"[ownValue]"`
	MergeValue  string `sql:"mergeValue" json:"[mergeValue]"`
	SpreadValue string `sql:"spreadValue" json:"[spreadValue]"`
}

type RealTimeDerivedViewRecord struct {
	ID       int    `sql:"id" json:"[id]"`
	OwnValue string `sql:"ownValue" json:"[ownValue]"`
	Data     string `sql:"data" json:"[data]"`
}

type RealTimeMergeViewRecord struct {
	MergeValue string `sql:"mergeValue"`
	Count      int    `sql:"count"`
}
type RealTimeSpreadViewRecord struct {
	Row         int    `sql:"row"`
	ID          int    `sql:"id"`
	OwnValue    string `sql:"ownValue"`
	SpreadValue string `sql:"spreadValue"`
}

type UniqueIdentifierViewRecord struct {
	Id string `sql:"id" json:"[id]"`
	FullName string `sql:"fullName" json:"[fullName]"`
}

var (
	developersRecords            []DeveloperRecord
	realTimeRecords              []RealTimeRecord
	realTimeDuplicateViewRecords []RealTimeDuplicateViewRecord
	realTimeDerivedViewRecords   []RealTimeDerivedViewRecord
	realTimeMergeViewRecords     []RealTimeMergeViewRecord
	realTimeSpreadViewRecords    []RealTimeSpreadViewRecord
)

const jobID = "test-job-id"

var loadRecords = new(sync.Once)

func getConnectedServer() pub.PublisherServer {

	sut := NewServer(log)
	settings := GetTestSettings()

	Expect(os.RemoveAll("./data")).To(Succeed())
	Expect(os.RemoveAll("./temp")).To(Succeed())
	Expect(sut.Configure(context.Background(), &pub.ConfigureRequest{
		LogLevel:           pub.LogLevel_Trace,
		PermanentDirectory: "./data",
		TemporaryDirectory: "./temp",
	}))
	Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())

	return sut
}

var _ = Describe("PublishStream with Real Time", func() {

	var (
		stream  *publisherStream
		timeout = 1 * time.Second
	)
	envTimeout, ok := os.LookupEnv("TEST_TIMEOUT")
	if ok {
		if envTimeoutDuration, err := time.ParseDuration(envTimeout); err == nil {
			timeout = envTimeoutDuration
		}
	}

	BeforeEach(func() {

		loadRecords.Do(func() {
			rows, err := db.Query("SELECT * FROM w3.dbo.RealTime")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeRecords)).To(Succeed())

			rows, err = db.Query("SELECT * FROM w3.dbo.RealTimeDuplicateView")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeDuplicateViewRecords)).To(Succeed())

			rows, err = db.Query("SELECT * FROM w3.dbo.RealTimeDerivedView")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeDerivedViewRecords)).To(Succeed())

			rows, err = db.Query("SELECT * FROM w3.dbo.RealTimeMergeView")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeMergeViewRecords)).To(Succeed())

			rows, err = db.Query("SELECT * FROM w3.dbo.RealTimeSpreadView")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeSpreadViewRecords)).To(Succeed())

			rows, err = db.Query("SELECT * FROM w3.dev.Developers")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &developersRecords)).To(Succeed())
		})

		stream = &publisherStream{
			out: make(chan *pub.Record),
		}
	})

	var discoverShape = func(sut pub.PublisherServer, schema *pub.Schema) *pub.Schema {
		response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverSchemasRequest{
			Mode:       pub.DiscoverSchemasRequest_REFRESH,
			SampleSize: 0,
			ToRefresh: []*pub.Schema{
				schema,
			},
		})
		Expect(err).ToNot(HaveOccurred())
		var out *pub.Schema
		for _, s := range response.Schemas {
			if s.Id == schema.Id {
				out = s
			}
		}
		Expect(out).ToNot(BeNil(), "should have discovered requested schema %q in %+v", schema.Id, response.Schemas)
		Expect(out.Errors).To(BeEmpty())
		return out
	}

	It("should assert correctly", func() {
		expected := RealTimeRecord{ID: 1, OwnValue: "a1", MergeValue: "a-x", SpreadValue: "a-z"}
		j, _ := json.MarshalIndent(expected, "", "  ")
		Expect(&pub.Record{
			Action:   pub.Record_UPSERT,
			DataJson: string(j),
		}).To(BeRecordMatching(pub.Record_UPSERT, expected))
	})

	It("should work with custom types", func() {

		// capture sut for goroutines in this test
		sut := getConnectedServer()

		schema := discoverShape(sut, &pub.Schema{Id: "[HumanResources].[Employee]"})
		settings := RealTimeSettings{PollingInterval: "100ms"}
		done := make(chan struct{})

		go func() {
			defer GinkgoRecover()
			defer close(done)
			err := sut.PublishStream(&pub.ReadRequest{
				JobId:                jobID,
				Schema:               schema,
				RealTimeSettingsJson: settings.String(),
				RealTimeStateJson:    "",
			}, stream)
			Expect(err).ToNot(HaveOccurred())

		}()

		expectedVersion := getChangeTrackingVersion()

		By("detecting that no state exists, all records should be loaded", func() {
			for i := 0; i < 4; i++ {
				var actualRecord *pub.Record
				Eventually(stream.out, timeout).Should(Receive(&actualRecord))
				Expect(actualRecord.Action).To(Equal(pub.Record_INSERT), fmt.Sprintf("for record %d", i))
			}
		})

		By("committing most recent version, the state should be stored", func() {

			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeARealTimeStateCommit(RealTimeState{Versions: map[string]int{
				"[HumanResources].[Employee]": expectedVersion,
			}}))
		})

		By("detecting inserted data", func() {
			Expect(db.Exec(`INSERT INTO HumanResources.Employee (BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, JobTitle, BirthDate, MaritalStatus, Gender, HireDate, SalariedFlag, VacationHours, SickLeaveHours, CurrentFlag, rowguid, ModifiedDate) VALUES (5, '695256908', 'adventure-works\gail0', 0x5ADA, 'Design Engineer', '1931-10-13', 'M', 'F', '2002-02-06', 1, 5, 22, 1, 'EC84AE09-F9B8-4A15-B4A9-6CCBAB919B08', '2008-07-31 00:00:00.000');`)).ToNot(BeNil())
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord.Action).To(Equal(pub.Record_INSERT))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Insert in [HumanResources].[Employee] at [BusinessEntityID]=%d", 5)))
		})

		By("committing most recent version, the state should be stored", func() {
			expectedVersion = getChangeTrackingVersion()
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeARealTimeStateCommit(RealTimeState{Versions: map[string]int{
				"[HumanResources].[Employee]": expectedVersion,
			}}))
		})

		By("running the publish periodically, multiple changed record should be detected", func() {
			result, err := db.Exec("UPDATE HumanResources.Employee SET JobTitle = 'Chief Hamster' WHERE BusinessEntityID = @id", sql.Named("id", 4))
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RowsAffected()).To(BeNumerically("==", 1))
			result, err = db.Exec("UPDATE HumanResources.Employee SET JobTitle = 'Chief Weasel' WHERE BusinessEntityID = @id", sql.Named("id", 5))
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RowsAffected()).To(BeNumerically("==", 1))

			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord.Action).To(Equal(pub.Record_UPDATE))
			var rawData map[string]interface{}
			Expect(actualRecord.UnmarshalData(&rawData)).To(Succeed())
			Expect(rawData).To(HaveKeyWithValue("[JobTitle]", "Chief Weasel"))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Update in [HumanResources].[Employee] at [BusinessEntityID]=%d", 5)))
		})

		Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())
		Eventually(done).Should(BeClosed())
	})

	// the mapper parameter is a function which converts a dependency record into a schema record
	DescribeTable("simple real time", func(shape *pub.Schema, settings RealTimeSettings,
		mapper func(r RealTimeRecord) interface{}) {

		// capture sut for goroutines in this test
		sut := getConnectedServer()

		schema := discoverShape(sut, shape)

		var (
			expectedInsertedRecord RealTimeRecord
			expectedUpdatedRecord  RealTimeRecord
			expectedDeletedRecord  RealTimeRecord
		)

		expectedVersion := getChangeTrackingVersion()
		done := make(chan struct{})

		go func() {
			defer GinkgoRecover()
			defer close(done)
			err := sut.PublishStream(&pub.ReadRequest{
				JobId:                jobID,
				Schema:               schema,
				RealTimeSettingsJson: settings.String(),
				RealTimeStateJson:    "",
			}, stream)
			Expect(err).ToNot(HaveOccurred())
		}()

		defer func() {
			Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())
		}()

		By("detecting that no state exists, all records should be loaded", func() {
			for _, expected := range realTimeRecords {
				var actualRecord *pub.Record
				Eventually(stream.out, timeout).Should(Receive(&actualRecord))
				Expect(actualRecord).To(BeRecordMatching(pub.Record_INSERT, mapper(expected)))
			}
		})

		By("committing most recent version, the state should be stored", func() {
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeARealTimeStateCommit(RealTimeState{Versions: map[string]int{
				"[RealTime]": expectedVersion,
			}}))
		})

		var actualID int64
		By("running the publish periodically, a new record should be detected when it is written", func() {

			row := db.QueryRow("INSERT INTO RealTime VALUES ('inserted', NULL, NULL); SELECT SCOPE_IDENTITY()")
			Expect(row.Scan(&actualID)).To(Succeed())
			expectedInsertedRecord = RealTimeRecord{
				ID:       int(actualID),
				OwnValue: "inserted",
			}
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeRecordMatching(pub.Record_INSERT, mapper(expectedInsertedRecord)))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Insert in [RealTime] at [id]=%d", actualID)))
		})

		By("committing most recent version, the state should be stored", func() {
			expectedVersion = getChangeTrackingVersion()
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeARealTimeStateCommit(RealTimeState{Versions: map[string]int{
				"[RealTime]": expectedVersion,
			}}))
		})

		By("running the publish periodically, a changed record should be detected when it is updated", func() {

			result, err := db.Exec("UPDATE RealTime SET ownValue = 'updated' WHERE id = @id", sql.Named("id", actualID))
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RowsAffected()).To(BeNumerically("==", 1))
			expectedUpdatedRecord = RealTimeRecord{
				ID:       int(actualID),
				OwnValue: "updated",
			}
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeRecordMatching(pub.Record_UPDATE, mapper(expectedUpdatedRecord)))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Update in [RealTime] at [id]=%d", actualID)))
		})

		By("committing most recent version, the state should be stored", func() {
			expectedVersion = getChangeTrackingVersion()
			Eventually(stream.out, timeout).Should(Receive(BeARealTimeStateCommit(RealTimeState{Versions: map[string]int{
				"[RealTime]": expectedVersion,
			}})))
		})

		By("running the publish periodically, a deleted record should be detected when it is deleted", func() {

			result, err := db.Exec("DELETE RealTime WHERE id = @id", sql.Named("id", actualID))
			Expect(err).ToNot(HaveOccurred())
			Expect(result.RowsAffected()).To(BeNumerically("==", 1))
			expectedDeletedRecord = RealTimeRecord{
				ID: int(actualID),
			}
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeRecordMatching(pub.Record_DELETE, mapper(expectedDeletedRecord)))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Delete in [RealTime] at [id]=%d", actualID)))
		})

		By("storing commits correctly, each record should be published once", func() {
			expected := map[interface{}]int{
				toString(mapper(expectedInsertedRecord)): 1,
				toString(mapper(expectedUpdatedRecord)):  1,
				toString(mapper(expectedDeletedRecord)):  1,
			}
			for _, r := range realTimeRecords {
				expected[toString(mapper(r))] = 1
			}
			actual := map[interface{}]int{}
			for _, r := range stream.records {
				if r.Action != pub.Record_REAL_TIME_STATE_COMMIT {
					// this is some complicated crap to correctly unmarshal the data to the schema type
					target := mapper(RealTimeRecord{})
					actualValue := reflect.New(reflect.ValueOf(target).Type())
					actualData := actualValue.Interface()
					Expect(r.UnmarshalData(&actualData)).To(Succeed())
					dataString := toString(actualData)
					actual[dataString] = actual[dataString] + 1
				}
			}
			for k, v := range expected {
				Expect(actual).To(HaveKeyWithValue(k, v))
			}
		})

		Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())

		Eventually(done).Should(BeClosed())

	},
		Entry("when schema is table", &pub.Schema{Id: "[RealTime]"}, RealTimeSettings{PollingInterval: "100ms"},
			func(r RealTimeRecord) interface{} {
				return r
			}),
		Entry("when schema is view", &pub.Schema{
			Id: "[RealTimeDuplicateView]",
			Properties: []*pub.Property{
				{Id: "[recordID]", Name: "recordID", IsKey: true},
				{Id: "[ownValue]"},
				{Id: "[mergeValue]"},
				{Id: "[spreadValue]"},
			},
		}, RealTimeSettings{
			PollingInterval: "100ms",
			Tables: []RealTimeTableSettings{
				{
					SchemaID: "[RealTime]",
					Query: `SELECT [RealTimeDuplicateView].recordID as [Schema.recordID], [RealTime].id as [Dependency.id]
FROM RealTimeDuplicateView
JOIN RealTime on [RealTimeDuplicateView].recordID = [RealTime].id`,
				},
			},
		}, func(r RealTimeRecord) interface{} {
			return RealTimeDuplicateViewRecord{
				RecordID:    r.ID,
				MergeValue:  r.MergeValue,
				OwnValue:    r.OwnValue,
				SpreadValue: r.SpreadValue,
			}
		}),
		Entry("when schema is query", &pub.Schema{
			Id:    "duplicateQuery",
			Query: "select * from RealTime",
		}, RealTimeSettings{
			PollingInterval: "100ms",
			Tables: []RealTimeTableSettings{
				{
					SchemaID: "[RealTime]",
					Query:    `SELECT [RealTime].id as [Schema.id], [RealTime].id as [Dependency.id] FROM RealTime`,
				},
			},
		}, func(r RealTimeRecord) interface{} {
			return r
		}),
	)

	Describe("large change sets", func() {

		It("should be able to handle large change sets", func() {

			// capture sut for goroutines in this test
			sut := getConnectedServer()

			schema := discoverShape(sut, &pub.Schema{Id: "[dev].[Developers]"})
			settings := RealTimeSettings{PollingInterval: "1s"}
			done := make(chan struct{})

			expectedVersion := getChangeTrackingVersion()

			go func() {
				defer GinkgoRecover()
				defer close(done)
				err := sut.ReadStream(&pub.ReadRequest{
					JobId:                jobID,
					Schema:               schema,
					RealTimeSettingsJson: settings.String(),
					RealTimeStateJson:    "",
				}, stream)
				Expect(err).ToNot(HaveOccurred())
			}()

			defer func() {
				Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())
			}()

			By("detecting that no state exists, all records should be loaded", func() {
				for _, expected := range developersRecords {
					var actualRecord *pub.Record
					Eventually(stream.out, timeout).Should(Receive(&actualRecord))
					Expect(actualRecord).To(BeRecordMatching(pub.Record_INSERT, expected))
				}
			})

			By("committing most recent version, the state should be stored", func() {
				var actualRecord *pub.Record
				Eventually(stream.out, timeout).Should(Receive(&actualRecord))
				Expect(actualRecord).To(BeARealTimeStateCommit(RealTimeState{Versions: map[string]int{
					"[dev].[Developers]": expectedVersion,
				}}))
			})

			By("running the publish periodically, a new record should be detected when it is written", func() {
				var queries []string
				expectedCount := 1010
				start := 1000
				for i := start; i < start+expectedCount; i++ {
					queries = append(queries, fmt.Sprintf("INSERT INTO dev.Developers VALUES (%d, 'test+%d')", i, i))
				}

				query := fmt.Sprintf(`
BEGIN TRANSACTION
%s
COMMIT TRANSACTION
`, strings.Join(queries, "\n"))

				Expect(db.Exec(query)).ToNot(BeNil())

				doneCh := make(chan struct{})
				go func() {

					for j := 0; j < expectedCount; j++ {
						_ = <-stream.out
						// if j % 100 == 0 || j > 1000{
						// 	fmt.Printf("%d/%d - record: %v\n", j, expectedCount, record)
						// }
					}
					// fmt.Printf("Receieved all records")
					close(doneCh)
				}()

				Eventually(doneCh, 10*time.Second).Should(BeClosed())
			})

			Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())

			Eventually(done, 3*time.Second).Should(BeClosed())

		})

	})

	DescribeTable("black box", func(testCase RealTimeTestCase) {

		// capture sut for goroutines in this test
		sut := getConnectedServer()

		schema := discoverShape(sut, testCase.Schema)

		done := make(chan struct{})

		testCase.Setup()

		go func() {
			defer GinkgoRecover()
			defer close(done)
			err := sut.PublishStream(&pub.ReadRequest{
				JobId:                jobID,
				Schema:               schema,
				RealTimeSettingsJson: testCase.RealTimeSettings.String(),
				RealTimeStateJson:    "",
			}, stream)
			Expect(err).ToNot(HaveOccurredStack())
		}()

		defer func() {
			Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())
		}()

		var records []*pub.Record

		for {
			var record *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&record))
			if record.Action == pub.Record_REAL_TIME_STATE_COMMIT {
				break
			} else {
				records = append(records, record)
			}
		}

		state := make(map[string]interface{})
		for _, action := range testCase.Actions {

			action(state)
			for {
				var record *pub.Record
				Eventually(stream.out, timeout).Should(Receive(&record))
				if record.Action == pub.Record_REAL_TIME_STATE_COMMIT {
					break
				} else {
					records = append(records, record)
				}
			}
		}

		Eventually(func() error {
			return testCase.Expectation(state, records)
		}).Should(Succeed())

		Expect(sut.Disconnect(context.Background(), &pub.DisconnectRequest{})).ToNot(BeNil())

		Eventually(done).Should(BeClosed())
	},
		Entry("publish from table", RealTimeTestCase{
			Schema:           &pub.Schema{Id: "[RealTime]"},
			RealTimeSettings: RealTimeSettings{PollingInterval: "100ms"},
			Setup: func() {
				Expect(db.Exec(`DELETE w3.dbo.RealTime`)).ToNot(BeNil())
				Expect(db.Exec(`DBCC CHECKIDENT ('w3.dbo.RealTime', RESEED, 0)`)).ToNot(BeNil())
				Expect(db.Exec(`INSERT INTO w3.dbo.RealTime
VALUES ('a1', 'a', NULL),
       ('a2', 'a', NULL),
       ('b1', 'b', NULL),
       ('b2', 'b', NULL),
       ('c1', NULL, 'c'),
       ('c2', NULL, 'c')`)).ToNot(BeNil())

			},
			Actions: []func(state map[string]interface{}){
				func(state map[string]interface{}) {
					var actualID int
					row := db.QueryRow("INSERT INTO RealTime VALUES ('inserted', NULL, NULL); SELECT SCOPE_IDENTITY()")
					Expect(row.Scan(&actualID)).To(Succeed())
					state["inserted"] = RealTimeRecord{
						ID:       int(actualID),
						OwnValue: "inserted",
					}
				},
				func(state map[string]interface{}) {
					result, err := db.Exec("UPDATE RealTime SET ownValue = 'updated' WHERE id = 1")
					Expect(err).ToNot(HaveOccurred())
					Expect(result.RowsAffected()).To(BeNumerically("==", 1))
					state["updated"] = RealTimeRecord{
						ID:         1,
						MergeValue: "a",
						OwnValue:   "updated",
					}
				},
				func(state map[string]interface{}) {
					result, err := db.Exec("DELETE RealTime WHERE id = 6")
					Expect(err).ToNot(HaveOccurred())
					Expect(result.RowsAffected()).To(BeNumerically("==", 1))
					state["deleted"] = RealTimeRecord{
						ID: 6,
					}
				},
			},
			Expectation: func(state map[string]interface{}, records []*pub.Record) error {

				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_INSERT, state["inserted"])))
				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_UPDATE, state["updated"])))
				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_DELETE, state["deleted"])))

				return nil
			},
		}),
		Entry("publish from view", RealTimeTestCase{
			Schema: &pub.Schema{
				Id: "[RealTimeDuplicateView]",
				Properties: []*pub.Property{
					{Id: "[recordID]", Name: "recordID", IsKey: true},
					{Id: "[ownValue]"},
					{Id: "[mergeValue]"},
					{Id: "[spreadValue]"},
				},
			},
			RealTimeSettings: RealTimeSettings{
				PollingInterval: "100ms",
				Tables: []RealTimeTableSettings{
					{
						SchemaID: "[RealTime]",
						Query: `SELECT [RealTimeDuplicateView].recordID as [Schema.recordID], [RealTime].id as [Dependency.id]
FROM RealTimeDuplicateView
JOIN RealTime on [RealTimeDuplicateView].recordID = [RealTime].id`,
					},
				},
			},

			Setup: func() {
				Expect(db.Exec(`DELETE w3.dbo.RealTime`)).ToNot(BeNil())
				Expect(db.Exec(`DBCC CHECKIDENT ('w3.dbo.RealTime', RESEED, 0)`)).ToNot(BeNil())
				Expect(db.Exec(`INSERT INTO w3.dbo.RealTime
VALUES ('a1', 'a', NULL),
       ('a2', 'a', NULL),
       ('b1', 'b', NULL),
       ('b2', 'b', NULL),
       ('c1', NULL, 'c'),
       ('c2', NULL, 'c')`)).ToNot(BeNil())

			},
			Actions: []func(state map[string]interface{}){
				func(state map[string]interface{}) {
					var actualID int
					row := db.QueryRow("INSERT INTO RealTime VALUES ('inserted', NULL, NULL); SELECT SCOPE_IDENTITY()")
					Expect(row.Scan(&actualID)).To(Succeed())
					state["inserted"] = RealTimeDuplicateViewRecord{
						RecordID: int(actualID),
						OwnValue: "inserted",
					}
				},
				func(state map[string]interface{}) {
					result, err := db.Exec("UPDATE RealTime SET ownValue = 'updated' WHERE id = 1")
					Expect(err).ToNot(HaveOccurred())
					Expect(result.RowsAffected()).To(BeNumerically("==", 1))
					state["updated"] = RealTimeDuplicateViewRecord{
						RecordID:   1,
						MergeValue: "a",
						OwnValue:   "updated",
					}
				},
				func(state map[string]interface{}) {
					result, err := db.Exec("DELETE RealTime WHERE id = 6")
					Expect(err).ToNot(HaveOccurred())
					Expect(result.RowsAffected()).To(BeNumerically("==", 1))
					state["deleted"] = RealTimeDuplicateViewRecord{
						RecordID: 6,
					}
				},
			},
			Expectation: func(state map[string]interface{}, records []*pub.Record) error {

				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_INSERT, state["inserted"])))
				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_UPDATE, state["updated"])))
				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_DELETE, state["deleted"])))

				return nil
			},
		}),

		Entry("publish from view with uniqueidentifier keys", RealTimeTestCase{
			Schema: &pub.Schema{
				Id: "[UniqueIdentifierView]",
				Properties: []*pub.Property{
					{Id: "[id]", Name: "id", IsKey: true},
					{Id: "[fullName]"},
				},
			},
			RealTimeSettings: RealTimeSettings{
				PollingInterval: "100ms",
				Tables: []RealTimeTableSettings{
					{
						SchemaID: "[UniqueIdentifier]",
						Query: `SELECT [UniqueIdentifierView].id as [Schema.id], [UniqueIdentifier].id as [Dependency.id]
FROM UniqueIdentifierView
JOIN UniqueIdentifier on [UniqueIdentifierView].id = [UniqueIdentifier].id`,
					},
				},
			},

			Setup: func() {

			},
			Actions: []func(state map[string]interface{}){
				func(state map[string]interface{}) {
					result, err := db.Exec("UPDATE [UniqueIdentifier] SET [firstName] = 'updated' WHERE id = @id", sql.Named("id", uniqueIdentifierIdSteve))
					Expect(err).ToNot(HaveOccurred())
					Expect(result.RowsAffected()).To(BeNumerically("==", 1))
					state["updated"] = UniqueIdentifierViewRecord{
						Id: uniqueIdentifierIdSteve,
						FullName: "updated ruble",
					}
				},
			},
			Expectation: func(state map[string]interface{}, records []*pub.Record) error {

				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_UPDATE, state["updated"])))

				return nil
			},
		}),
		Entry("publish from user defined query", RealTimeTestCase{
			Schema: &pub.Schema{
				// Id: "user-defined-query-x",
				Properties: []*pub.Property{
					{Id: "[id]", Name: "id", IsKey: true, Type: pub.PropertyType_INTEGER, TypeAtSource: "int"},
					{Id: "[ownValue]", Type: pub.PropertyType_STRING, TypeAtSource: "varchar(10)"},
					{Id: "[mergeValue]", Type: pub.PropertyType_STRING, TypeAtSource: "varchar(10)"},
					{Id: "[spreadValue]", Type: pub.PropertyType_STRING, TypeAtSource: "varchar(10)"},
				},
				Query: `SELECT * from [RealTime]`,
			},
			RealTimeSettings: RealTimeSettings{
				PollingInterval: "100ms",
				Tables: []RealTimeTableSettings{
					{
						SchemaID: "[RealTime]",
						Query: `SELECT [RealTime].id as [Schema.id], [RealTime].id as [Dependency.id]
FROM [RealTime]`,
					},
				},
			},

			Setup: func() {
				Expect(db.Exec(`DELETE w3.dbo.RealTime`)).ToNot(BeNil())
				Expect(db.Exec(`DBCC CHECKIDENT ('w3.dbo.RealTime', RESEED, 0)`)).ToNot(BeNil())
				Expect(db.Exec(`INSERT INTO w3.dbo.RealTime
VALUES ('a1', 'a', NULL),
       ('a2', 'a', NULL),
       ('b1', 'b', NULL),
       ('b2', 'b', NULL),
       ('c1', NULL, 'c'),
       ('c2', NULL, 'c')`)).ToNot(BeNil())

			},
			Actions: []func(state map[string]interface{}){
				func(state map[string]interface{}) {
					var actualID int
					row := db.QueryRow("INSERT INTO RealTime VALUES ('inserted', NULL, NULL); SELECT SCOPE_IDENTITY()")
					Expect(row.Scan(&actualID)).To(Succeed())
					state["inserted"] = RealTimeRecord{
						ID:       int(actualID),
						OwnValue: "inserted",
					}
				},
				func(state map[string]interface{}) {
					result, err := db.Exec("UPDATE RealTime SET ownValue = 'updated' WHERE id = 1")
					Expect(err).ToNot(HaveOccurred())
					Expect(result.RowsAffected()).To(BeNumerically("==", 1))
					state["updated"] = RealTimeRecord{
						ID:         1,
						MergeValue: "a",
						OwnValue:   "updated",
					}
				},
				func(state map[string]interface{}) {
					result, err := db.Exec("DELETE RealTime WHERE id = 6")
					Expect(err).ToNot(HaveOccurred())
					Expect(result.RowsAffected()).To(BeNumerically("==", 1))
					state["deleted"] = RealTimeRecord{
						ID: 6,
					}
				},
			},
			Expectation: func(state map[string]interface{}, records []*pub.Record) error {

				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_INSERT, state["inserted"])))
				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_UPDATE, state["updated"])))
				Expect(records).To(ContainElement(BeRecordMatching(pub.Record_DELETE, state["deleted"])))

				return nil
			},
		}),
	)
})

type RealTimeTestCase struct {
	Schema           *pub.Schema
	RealTimeSettings RealTimeSettings
	Setup            func()
	Actions          []func(state map[string]interface{})
	Expectation func(state map[string]interface{}, records []*pub.Record) error
}

type recordExpectation struct {
	Action pub.Record_Action
	Data   interface{}
}

func BeRecordMatching(action pub.Record_Action, data interface{}) GomegaMatcher {

	l := log.Named("BeRecordMatching")

	var expectedData map[string]interface{}
	j, _ := json.Marshal(data)
	_ = json.Unmarshal(j, &expectedData)
	for k, v := range expectedData {
		switch x := v.(type) {
		case string:
			if x == "" {
				expectedData[k] = nil
			}
		}
	}

	expected := recordExpectation{
		Action: action,
		Data:   expectedData,
	}

	return WithTransform(func(r *pub.Record) recordExpectation {
		var actualData map[string]interface{}
		Expect(r.UnmarshalData(&actualData)).To(Succeed())
		actual := recordExpectation{
			Action: r.Action,
			Data:   actualData,
		}
		l.Trace("Checking equivalency", "expected", expected, "actual", actual, "source", r)
		return actual
	}, BeEquivalentTo(expected))

}

func BeARealTimeStateCommit(expected RealTimeState) GomegaMatcher {
	return WithTransform(func(r *pub.Record) RealTimeState {
		// Expect(r.Action).To(BeEquivalentTo(pub.Record_REAL_TIME_STATE_COMMIT),"expected a real time state commit, not %s", r.Action)
		var actual RealTimeState
		Expect(r.UnmarshalRealTimeState(&actual)).To(Succeed())
		return actual
	}, BeEquivalentTo(expected))
}

func getChangeTrackingVersion() int {
	row := db.QueryRow(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`)
	var version int
	ExpectWithOffset(1, row.Scan(&version)).To(Succeed())
	return version
}

func toString(i interface{}) string {
	j, _ := json.Marshal(i)
	var m map[string]interface{}
	_ = json.Unmarshal(j, &m)
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		out = append(out, fmt.Sprintf("%s:%v", k, m[k]))
	}

	return strings.Join(out, "; ")
}

const uniqueIdentifierIdSteve = "0E984725-FACE-4BF4-9960-E1C80E27ABA0"
const uniqueIdentifierIdCasey = "6F9619FF-CAFE-D011-B42D-00C04FC964FF"
