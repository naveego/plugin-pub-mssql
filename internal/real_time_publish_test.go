package internal_test

import (
	"context"
	"github.com/hashicorp/go-hclog"
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/types"
	"sync"
	"time"
)

type RealTimeRecord struct {
	ID          int    `sql:"id" json:"[id]"`
	OwnValue    string `sql:"ownValue" json:"[ownValue]"`
	MergeValue  string `sql:"mergeValue" json:"[mergeValue]"`
	SpreadValue string `sql:"spreadValue" json:"[spreadValue]"`
}

type RealTimeDirectViewRecord struct {
	ID       int    `sql:"id"`
	OwnValue string `sql:"ownValue"`
	Data     int    `sql:"data"`
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
	realTimeRecords            []RealTimeRecord
	realTimeDirectViewRecords  []RealTimeDirectViewRecord
	realTimeDerivedViewRecords []RealTimeDerivedViewRecord
	realTimeMergeViewRecords   []RealTimeMergeViewRecord
	realTimeSpreadViewRecords  []RealTimeSpreadViewRecord
)

var loadRecords = new(sync.Once)

var _ = FDescribe("PublishStream with Real Time", func() {

	var (
		sut      pub.PublisherServer
		settings Settings
		stream   *publisherStream
	)

	BeforeEach(func() {

		log := hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Trace,
			Output:     GinkgoWriter,
			JSONFormat: true,
		})

		loadRecords.Do(func() {
			rows, err := db.Query("select * from w3.dbo.RealTime")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeRecords)).To(Succeed())

			rows, err = db.Query("select * from w3.dbo.RealTimeDirectView")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeDirectViewRecords)).To(Succeed())

			rows, err = db.Query("select * from w3.dbo.RealTimeDerivedView")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeDerivedViewRecords)).To(Succeed())

			rows, err = db.Query("select * from w3.dbo.RealTimeMergeView")
			Expect(err).ToNot(HaveOccurred())
			Expect(sqlstructs.UnmarshalRows(rows, &realTimeMergeViewRecords)).To(Succeed())

			rows, err = db.Query("select * from w3.dbo.RealTimeSpreadView")
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
		Expect(sut.Connect(context.Background(), pub.NewConnectRequest(settings))).ToNot(BeNil())
	})

	AfterEach(func() {
		Expect(sut.Disconnect(context.Background(), new(pub.DisconnectRequest))).ToNot(BeNil())
	})

	var discoverShape = func(schemaID string) *pub.Shape {
		response, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
			Mode:       pub.DiscoverShapesRequest_ALL,
			SampleSize: 0,
		})
		Expect(err).ToNot(HaveOccurred())
		var out *pub.Shape
		for _, s := range response.Shapes {
			if s.Id == schemaID {
				out = s
			}
		}
		Expect(out).ToNot(BeNil(), "should have discovered requested schema %q in %+v", schemaID, response.Shapes)
		return out
	}

	Describe("when schema is table", func() {
		var (
			schema *pub.Shape
		)
		BeforeEach(func() {
			schema = discoverShape("[RealTime]")
		})

		Describe("when there is no commit state", func() {

			It("should behave as expected", func() {

				expectedVersion := getChangeTrackingVersion()

				go func() {
					defer GinkgoRecover()
					err := sut.PublishStream(&pub.PublishRequest{
						Shape: schema,
						RealTimeSettingsJson: RealTimeSettings{
							PollingInterval: (100 * time.Millisecond).String(),
						}.String(),
						RealTimeStateJson: "",
					}, stream)
					Expect(err).ToNot(HaveOccurred())
				}()

				By("Initial load of records", func() {
					for _, expected := range realTimeRecords {
						Eventually(stream.out).Should(Receive(ARealTimeRecord(expected)))
					}
				})

				By("Committing most recent version", func() {
					Eventually(stream.out).Should(Receive(ARealTimeStateCommit(RealTimeState{Version:expectedVersion})))
				})

				var actualID int64
				By("Detecting a new record is written.", func() {

					row := db.QueryRow("insert into RealTime values ('inserted', null, null); SELECT SCOPE_IDENTITY()")
					Expect(row.Scan(&actualID)).To(Succeed())
					expected := RealTimeRecord{
						ID:int(actualID),
						OwnValue:"inserted",
					}
					Eventually(stream.out).Should(Receive(ARealTimeRecord(expected)))

					Expect(stream.records).To(HaveOneOfEachRealTimeRecord())
				})

			})

		})

	})

})

func HaveOneOfEachRealTimeRecord() GomegaMatcher {
	return WithTransform(func(records []*pub.Record) []int {
		counts := make([]int, len(records))
		for _, r := range records {
			var actual RealTimeRecord
			_ = r.UnmarshalData(&actual)
			counts[actual.ID]++
		}
		return counts
	}, Not(ContainElement(BeNumerically(">", 1))))
}

func ARealTimeRecord(expected RealTimeRecord) GomegaMatcher {
	return WithTransform(func(r *pub.Record) RealTimeRecord {
		var actual RealTimeRecord
		Expect(r.UnmarshalData(&actual)).To(Succeed())
		return actual
	}, BeEquivalentTo(expected))
}
func ARealTimeStateCommit(expected RealTimeState) GomegaMatcher {
	return WithTransform(func(r *pub.Record) RealTimeState {
		Expect(r.Action).To(BeEquivalentTo(pub.Record_REAL_TIME_STATE_COMMIT),"expected a real time state commit, not %s", r.Action)
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
