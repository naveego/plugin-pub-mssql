package internal_test

import (
	. "github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	. "github.com/naveego/plugin-pub-mssql/pkg/store"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
)

const Path = "./test/db"

var _ = Describe("Transaction Helper", func() {

	var sut BoltStore

	BeforeEach(func(){
		var err error
		Expect(os.MkdirAll(filepath.Dir(Path), 0700)).To(Succeed())
		sut, err = GetBoltStore(Path)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func(){
		Expect(DestroyBoltStore(sut)).To(Succeed())
	})

	It("should replace schema correctly", func(){
		schemaID := "schema-id"
		rowKeys := meta.RowKeys{
			{ColumnID:"a",Value:"A"},
			{ColumnID:"b",Value:"B"},
		}
		rowValues := meta.RowValues{
			{ColumnID:"x", Value:"X"},
			{ColumnID:"y", Value:"Y"},
		}

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			result, err := sut.SetSchemaHash(schemaID, rowKeys, rowValues)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(StoreAdded))
			return nil
		})).To(Succeed(), "add should succeed")

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			result, err := sut.SetSchemaHash(schemaID, rowKeys, rowValues)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(StoreNoop))
			return nil
		})).To(Succeed(), "add should do nothing when there is no change")

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			result, err := sut.SetSchemaHash(schemaID, rowKeys, meta.RowValues{
				{ColumnID:"x", Value:"X1"},
				{ColumnID:"y", Value:"Y1"},
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(StoreChanged))
			return nil
		})).To(Succeed(), "update should succeed")


		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			result, err := sut.SetSchemaHash(schemaID, rowKeys, nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(StoreDeleted))
			return nil
		})).To(Succeed(), "delete should succeed")

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			result, err := sut.SetSchemaHash(schemaID, rowKeys, rowValues)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(StoreAdded))
			return nil
		})).To(Succeed(), "add should succeed after delete")


	})

	It("should register dependencies correctly", func(){
		schemaID := "schema-id"
		depID := "dep-id"
		depKeys := meta.RowKeys{{ColumnID:"x", Value:"X"}}
		schemaAKeys := meta.RowKeys{
			{ColumnID:"a",Value:"A"},
		}
		schemaBKeys := meta.RowKeys{
			{ColumnID:"b",Value:"B"},
		}
		schemaCKeys := meta.RowKeys{
			{ColumnID:"c",Value:"C"},
		}

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			Expect(sut.RegisterDependency(depID, schemaID, depKeys, schemaAKeys)).To(Succeed())
			Expect(sut.RegisterDependency(depID, schemaID, depKeys, schemaBKeys)).To(Succeed())
			Expect(sut.RegisterDependency(depID, schemaID, depKeys, schemaCKeys)).To(Succeed())
			return nil
		})).To(Succeed())

		Expect(sut.DB.View(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			Expect(sut.GetDependentSchemaKeys(depID, schemaID, depKeys)).To(ConsistOf(schemaAKeys, schemaBKeys, schemaCKeys))
			return nil
		})).To(Succeed())

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			Expect(sut.UnregisterDependency(depID, schemaID, depKeys, schemaAKeys)).To(Succeed())
			return nil
		})).To(Succeed(), "should be able to unregister single dependency")

		Expect(sut.DB.View(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			Expect(sut.GetDependentSchemaKeys(depID, schemaID, depKeys)).To(ConsistOf(schemaBKeys, schemaCKeys))
			return nil
		})).To(Succeed(), "schema A dependency was unregistered")

		Expect(sut.DB.Update(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			Expect(sut.UnregisterDependency(depID, schemaID,  depKeys, nil)).To(Succeed())
			return nil
		})).To(Succeed(), "should be able to unregister all dependencies")

		Expect(sut.DB.View(func(tx *bolt.Tx) error {
			sut := NewTxHelper(tx)
			Expect(sut.GetDependentSchemaKeys(depID, schemaID, depKeys)).To(BeEmpty())
			return nil
		})).To(Succeed(), "all dependencies were unregistered")
	})
})
