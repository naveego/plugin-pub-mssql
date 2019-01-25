package store_test

import (
	. "github.com/naveego/plugin-pub-mssql/pkg/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
)

const Path = "./test/db"

var _ = Describe("Bolt", func() {

	var sut BoltStore

	BeforeEach(func(){
		var err error
		Expect(os.MkdirAll(filepath.Dir(Path), 0700)).To(Succeed())
		sut, err = GetBoltStore(Path)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func(){
		Expect(ReleaseBoltStore(sut)).To(Succeed())
	})

	It("should set and get data", func(){
		expected := []byte("data")
		key := []byte("key")
		bucket := []byte("bucket")

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(bucket)
			Expect(err).ToNot(HaveOccurred())
			Expect(b.Put(key, expected)).ToNot(HaveOccurred())
			return nil
		})).To(Succeed())

		Expect(sut.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucket)
			Expect(b).ToNot(BeNil())
			Expect(b.Get(key)).To(Equal(expected))
			return nil
		})).To(Succeed())
	})
})
