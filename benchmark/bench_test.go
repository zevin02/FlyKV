package benchmark

import (
	"FlexDB"
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

const DirPath = "/home/zevin/githubmanage/program/FlexDB/fortest"

var db *FlexDB.DB

func init() {
	//初始化一个存储引擎对象
	opt := FlexDB.DefaultOperations
	opt.DirPath = DirPath
	opt.IndexType = FlexDB.Btree
	var err error
	db, err = FlexDB.Open(opt)
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
		if err != nil && err != FlexDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()   //重新计数
	b.ReportAllocs() //可以打印出内存分配的情况
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		_, err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}

}
