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
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/types"
	"os"
	"reflect"
	"sync"
	"time"
)

type RealTimeRecord struct {
	ID          int    `sql:"id" json:"[id]"`
	OwnValue    string `sql:"ownValue" json:"[ownValue]"`
	MergeValue  string `sql:"mergeValue" json:"[mergeValue]"`
	SpreadValue string `sql:"spreadValue" json:"[spreadValue]"`
}

type RealTimeDuplicateViewRecord struct {
	ID          int    `sql:"id" json:"[id]"`
	OwnValue    string `sql:"ownValue" json:"[ownValue]"`
	MergeValue  string `sql:"mergeValue" json:"[mergeValue]"`
	SpreadValue string `sql:"spreadValue" json:"[spreadValue]"`
}

type RealTimeDerivedViewRecord struct {
	OwnValue  string `sql:"ownValue"`
	IgnoredID int    `sql:"ignoredID"`
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

var (
	realTimeRecords              []RealTimeRecord
	realTimeDuplicateViewRecords []RealTimeDuplicateViewRecord
	realTimeDerivedViewRecords   []RealTimeDerivedViewRecord
	realTimeMergeViewRecords     []RealTimeMergeViewRecord
	realTimeSpreadViewRecords    []RealTimeSpreadViewRecord
)

const jobID = "test-job-id"

var loadRecords = new(sync.Once)

var _ = Describe("PublishStream with Real Time", func() {

	var (
		sut      pub.PublisherServer
		settings Settings
		stream   *publisherStream
		timeout  = 1 * time.Second
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

			rows, err = db.Query("SELECT * FROM w3.dbo.RealTimeDirectView")
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
		})

		sut = NewServer(log)

		settings = *GetTestSettings()

		stream = &publisherStream{
			out: make(chan *pub.Record),
		}
	})

	BeforeEach(func() {
		Expect(os.RemoveAll("./data")).To(Succeed())
		Expect(os.RemoveAll("./temp")).To(Succeed())
		Expect(sut.Configure(context.Background(), &pub.ConfigureRequest{
			LogLevel:           pub.LogLevel_Trace,
			PermanentDirectory: "./data",
			TemporaryDirectory: "./temp",
		}))
		Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
	})

	AfterEach(func() {
		Expect(sut.Disconnect(context.Background(), new(pub.DisconnectRequest))).ToNot(BeNil())
	})

	var discoverShape = func(schema *pub.Shape) *pub.Shape {
		response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
			Mode:       pub.DiscoverShapesRequest_REFRESH,
			SampleSize: 0,
			ToRefresh: []*pub.Shape{
				schema,
			},
		})
		Expect(err).ToNot(HaveOccurred())
		var out *pub.Shape
		for _, s := range response.Shapes {
			if s.Id == schema.Id {
				out = s
			}
		}
		Expect(out).ToNot(BeNil(), "should have discovered requested schema %q in %+v", schema.Id, response.Shapes)
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

	DescribeTable("simple real time", func(shape *pub.Shape, settings RealTimeSettings) {

		schema := discoverShape(shape)

		var (
			expectedInsertedRecord RealTimeRecord
			expectedUpdatedRecord  RealTimeRecord
			expectedDeletedRecord  RealTimeRecord
		)

		expectedVersion := getChangeTrackingVersion()

		go func() {
			defer GinkgoRecover()
			err := sut.PublishStream(&pub.PublishRequest{
				JobId:                jobID,
				Shape:                schema,
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
				Expect(actualRecord).To(BeRecordMatching(pub.Record_INSERT, expected))
			}
		})

		By("committing most recent version, the state should be stored", func() {
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeARealTimeStateCommit(RealTimeState{Version: expectedVersion}))
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
			Expect(actualRecord).To(BeRecordMatching(pub.Record_INSERT, expectedInsertedRecord))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Insert in [RealTime] at [id]=%d", actualID)))
		})

		By("committing most recent version, the state should be stored", func() {
			expectedVersion = getChangeTrackingVersion()
			var actualRecord *pub.Record
			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
			Expect(actualRecord).To(BeARealTimeStateCommit(RealTimeState{Version: expectedVersion}))
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
			Expect(actualRecord.Action).To(Equal(pub.Record_UPDATE))
			var actualRealTimeRecord RealTimeRecord
			Expect(actualRecord.UnmarshalData(&actualRealTimeRecord)).To(Succeed())
			Expect(actualRealTimeRecord).To(BeEquivalentTo(expectedUpdatedRecord))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Update in [RealTime] at [id]=%d", actualID)))
		})

		By("committing most recent version, the state should be stored", func() {
			expectedVersion = getChangeTrackingVersion()
			Eventually(stream.out, timeout).Should(Receive(BeARealTimeStateCommit(RealTimeState{Version: expectedVersion})))
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
			Expect(actualRecord.Action).To(Equal(pub.Record_DELETE))
			var actualRealTimeRecord RealTimeRecord
			Expect(actualRecord.UnmarshalData(&actualRealTimeRecord)).To(Succeed())
			Expect(actualRealTimeRecord).To(BeEquivalentTo(expectedDeletedRecord))
			Expect(actualRecord.Cause).To(ContainSubstring(fmt.Sprintf("Delete in [RealTime] at [id]=%d", actualID)))
		})

		By("storing commits correctly, each record should be published once", func() {
			expected := map[RealTimeRecord]int{
				expectedInsertedRecord: 1,
				expectedUpdatedRecord:  1,
				expectedDeletedRecord:  1,
			}
			for _, r := range realTimeRecords {
				expected[r] = 1
			}
			actual := map[RealTimeRecord]int{}
			for _, r := range stream.records {
				if r.Action != pub.Record_REAL_TIME_STATE_COMMIT {
					var a RealTimeRecord
					_ = r.UnmarshalData(&a)

					actual[a] = actual[a] + 1
				}
			}
			Expect(actual).To(BeEquivalentTo(expected))
		})

	},
		Entry("when schema is table", &pub.Shape{Id:"[RealTime]"}, RealTimeSettings{PollingInterval: "100ms"}),
		Entry("when schema is view", &pub.Shape{Id:"[RealTimeDuplicateView]"}, RealTimeSettings{
			PollingInterval: "100ms",
			Tables: []RealTimeTableSettings{
				{
					SchemaID: "[RealTime]",
					Query: `SELECT [RealTimeDuplicateView].id as [Schema.id], [RealTime].id as [Dependency.id]
FROM RealTimeDuplicateView
JOIN RealTime on [RealTimeDuplicateView].id = [RealTime].id`,
				},
			},
		}),
		Entry("when schema is query", &pub.Shape{
			Id:"duplicateQuery",
			Query:"select * from realtime",
		}, RealTimeSettings{
			PollingInterval: "100ms",
			Tables: []RealTimeTableSettings{
				{
					SchemaID: "[RealTime]",
					Query: `SELECT [RealTime].id as [Schema.id], [RealTime].id as [Dependency.id] FROM RealTime`,
				},
			},
		}),
	)

	//
	// XIt("should publish from a view", func() {
	//
	// 	schema := discoverShape(&pub.Shape{Id: "[RealTimeDirectView]"})
	//
	// 	var (
	// 		expectedInsertedRecord RealTimeDuplicateViewRecord
	// 		expectedUpdatedRecord  RealTimeDuplicateViewRecord
	// 		expectedDeletedRecord  RealTimeDuplicateViewRecord
	// 	)
	//
	// 	expectedVersion := getChangeTrackingVersion()
	//
	// 	go func() {
	// 		defer GinkgoRecover()
	// 		settings := RealTimeSettings{
	// 			KeyColumns:      []string{"id"},
	// 			PollingInterval: (100 * time.Millisecond).String(),
	// 			Tables: []RealTimeTableSettings{
	// 				{
	// 					SchemaID: "[RealTime]",
	// 					Query:    "SELECT id FROM [RealTime] as Source",
	// 				},
	// 			},
	// 		}
	// 		err := sut.PublishStream(&pub.PublishRequest{
	// 			Shape:                schema,
	// 			RealTimeSettingsJson: settings.String(),
	// 			RealTimeStateJson:    "",
	// 		}, stream)
	// 		Expect(err).ToNot(HaveOccurred())
	// 	}()
	//
	// 	By("detecting that no state exists, all records should be loaded", func() {
	// 		for _, expected := range realTimeDuplicateViewRecords {
	// 			var actualRecord *pub.Record
	// 			Eventually(stream.out, timeout).Should(Receive(&actualRecord))
	// 			Expect(actualRecord).To(BeRecordMatching(pub.Record_UPSERT, expected))
	// 		}
	// 	})
	//
	// 	By("committing most recent version, the state should be stored", func() {
	// 		Eventually(stream.out).Should(Receive(BeARealTimeStateCommit(RealTimeState{Version: expectedVersion})))
	// 	})
	//
	// 	var actualID int64
	// 	var auxID int64
	// 	By("running the publish periodically, a new record should be detected when it is written", func() {
	// 		row := db.QueryRow("INSERT INTO RealTime VALUES ('inserted', NULL, NULL); SELECT SCOPE_IDENTITY()")
	// 		Expect(row.Scan(&actualID)).To(Succeed())
	// 		row = db.QueryRow("INSERT INTO RealTimeAux VALUES (@realTimeID, 'ins-aux'); SELECT SCOPE_IDENTITY()", sql.Named("realTimeID", actualID))
	// 		Expect(row.Scan(&auxID)).To(Succeed())
	// 		expectedInsertedRecord = RealTimeDuplicateViewRecord{
	// 			ID:       int(actualID),
	// 			OwnValue: "inserted",
	// 			Data:     "ins-aux",
	// 		}
	// 		Eventually(stream.out, timeout).Should(Receive(BeRecordMatching(pub.Record_INSERT, expectedInsertedRecord)))
	// 	})
	//
	// 	By("committing most recent version, the state should be stored", func() {
	// 		expectedVersion = getChangeTrackingVersion()
	// 		Eventually(stream.out, timeout).Should(Receive(BeARealTimeStateCommit(RealTimeState{Version: expectedVersion})))
	// 	})
	//
	// 	By("running the publish periodically, a changed record should be detected when it is updated", func() {
	//
	// 		result, err := db.Exec("UPDATE RealTime SET ownValue = 'updated' WHERE id = @id", sql.Named("id", actualID))
	// 		Expect(err).ToNot(HaveOccurred())
	// 		Expect(result.RowsAffected()).To(BeNumerically("==", 1))
	// 		expectedUpdatedRecord = RealTimeDuplicateViewRecord{
	// 			ID:       int(actualID),
	// 			OwnValue: "updated",
	// 			Data:     "ins-aux",
	// 		}
	// 		var actualRecord *pub.Record
	// 		Eventually(stream.out, timeout).Should(Receive(&actualRecord))
	// 		Expect(actualRecord.Action).To(Equal(pub.Record_UPDATE))
	// 		var actualRealTimeRecord RealTimeDuplicateViewRecord
	// 		Expect(actualRecord.UnmarshalData(&actualRealTimeRecord)).To(Succeed())
	// 		Expect(actualRealTimeRecord).To(BeEquivalentTo(expectedUpdatedRecord))
	// 	})
	//
	// 	By("committing most recent version, the state should be stored", func() {
	// 		expectedVersion = getChangeTrackingVersion()
	// 		Eventually(stream.out, timeout).Should(Receive(BeARealTimeStateCommit(RealTimeState{Version: expectedVersion})))
	// 	})
	//
	// 	By("running the publish periodically, a deleted record should be detected when it is deleted", func() {
	//
	// 		result, err := db.Exec("DELETE RealTimeAux WHERE id = @id", sql.Named("id", auxID))
	// 		Expect(err).ToNot(HaveOccurred())
	// 		Expect(result.RowsAffected()).To(BeNumerically("==", 1))
	// 		expectedDeletedRecord = RealTimeDuplicateViewRecord{
	// 			ID: int(actualID),
	// 		}
	// 		var actualRecord *pub.Record
	// 		Eventually(stream.out, timeout).Should(Receive(&actualRecord))
	// 		Expect(actualRecord.Action).To(Equal(pub.Record_DELETE))
	// 		var actualRealTimeRecord RealTimeDuplicateViewRecord
	// 		Expect(actualRecord.UnmarshalData(&actualRealTimeRecord)).To(Succeed())
	// 		Expect(actualRealTimeRecord).To(BeEquivalentTo(expectedDeletedRecord))
	// 		Expect(actualRecord.Cause).To(ContainSubstring("[RealTime]"))
	// 		Expect(actualRecord.Cause).To(ContainSubstring("delete"))
	// 		Expect(actualRecord.Cause).To(ContainSubstring("7"))
	// 	})
	//
	// 	By("storing commits correctly, each record should be published once", func() {
	// 		expected := map[RealTimeDuplicateViewRecord]int{
	// 			expectedInsertedRecord: 1,
	// 			expectedUpdatedRecord:  1,
	// 			expectedDeletedRecord:  1,
	// 		}
	// 		for _, r := range realTimeDuplicateViewRecords {
	// 			expected[r] = 1
	// 		}
	// 		actual := map[RealTimeRecord]int{}
	// 		for _, r := range stream.records {
	// 			if r.Action != pub.Record_REAL_TIME_STATE_COMMIT {
	// 				var a RealTimeRecord
	// 				_ = r.UnmarshalData(&a)
	//
	// 				actual[a] = actual[a] + 1
	// 			}
	// 		}
	// 		Expect(actual).To(BeEquivalentTo(expected))
	// 	})
	//
	// })

})

type recordExpectation struct {
	Action pub.Record_Action
	Data   interface{}
}

func BeRecordMatching(action pub.Record_Action, data interface{}) GomegaMatcher {

	l := log.Named("BeRecordMatching")

	expectedValue := reflect.ValueOf(data)
	expected := recordExpectation{
		Action: action,
		Data:   data,
	}

	return WithTransform(func(r *pub.Record) recordExpectation {
		actualValue := reflect.New(expectedValue.Type())
		actualData := actualValue.Interface()
		Expect(r.UnmarshalData(&actualData)).To(Succeed())
		acv := reflect.ValueOf(actualData)
		actualData = acv.Elem().Interface()
		actual := recordExpectation{r.Action, actualData}
		l.Trace("Checking equivalency", "expected", expected, "actual", actual, "source", r)
		return actual
	}, BeEquivalentTo(expected))

}

func ResolveRecord(record *pub.Record, data interface{}, state interface{}) pub.ResolvedRecord {
	r, err := record.Resolve(&data, &state)
	ExpectWithOffset(1, err).ToNot(HaveOccurred())
	return r
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
