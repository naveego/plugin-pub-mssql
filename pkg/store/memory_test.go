package store_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/naveego/plugin-pub-mssql/pkg/store"

)

var _ = Describe("Memory", func() {
	Describe("AddOrUpdateSchemaRecord", func(){
		var sut Store
		BeforeEach(func(){
			var err error
			sut, err = GetStore("./test/data", StoreTypeMemory)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should return add when adding", func(){
			added, changed, err := sut.AddOrUpdateSchemaRecord("test", []byte("key"), []byte("hash"))
			Expect(added).To(BeTrue())
			Expect(changed).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should return correctly when key already exists with same value", func(){
			_, _, _ = sut.AddOrUpdateSchemaRecord("test", []byte("key"), []byte("hash"))
			added, changed, err := sut.AddOrUpdateSchemaRecord("test", []byte("key"), []byte("hash"))
			Expect(added).To(BeFalse())
			Expect(changed).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())
		})
		It("should return correctly when key already exists with changed value", func(){
			_, _, _ = sut.AddOrUpdateSchemaRecord("test", []byte("key"), []byte("hash1"))
			added, changed, err := sut.AddOrUpdateSchemaRecord("test", []byte("key"), []byte("hash2"))
			Expect(added).To(BeFalse())
			Expect(changed).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
