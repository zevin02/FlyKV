package benchmark

import (
	"BitcaskDB"
	"BitcaskDB/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

var db *BitcaskDB.DB

func init() {
	//初始化一个存储引擎对象
	opt := BitcaskDB.DefaultOperations
	opt.IndexType = BitcaskDB.Btree
	var err error
	db, err = BitcaskDB.Open(opt)
	if err != nil {
		panic(err)
	}

}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()   //重新计数
	b.ReportAllocs() //可以打印出内存分配的情况
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()   //重新计数
	b.ReportAllocs() //可以打印出内存分配的情况
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != BitcaskDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()   //重新计数
	b.ReportAllocs() //可以打印出内存分配的情况
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}

}
