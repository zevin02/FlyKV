package BitcaskDB

import (
	"BitcaskDB/utils"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

//没有任何数据的情况下进行merge
func TestDB_Merge(t *testing.T) {
	opts := DefaultOperations
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)

	err = db.Merge()
	assert.Nil(t, err)
}

//全部都是有效数据
func TestDB_Merge2(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0 //不设置失效的阈值
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)
	//重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	keys := db2.ListKeys()
	assert.Equal(t, 50000, len(keys))
	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}

}

//有失败数据和被重复put的数据
func TestDB_Merge3(t *testing.T) {
	opts := DefaultOperations
	//TODO test b+ index
	//opts.IndexType = BPT
	opts.FileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0 //不设置失效的阈值
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	//删除部分数据
	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}
	err = db.Merge()
	assert.Nil(t, err)
	//重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)

	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	keys := db2.ListKeys()
	//Merge的时候删除了数据
	assert.Equal(t, 40000, len(keys))
	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 40000; i < 10000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("new value in merge"), val)
	}
}

//全部是无效的数据
func TestDB_Merge4(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0 //不设置失效的阈值
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	//重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)

	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	keys := db2.ListKeys()
	//Merge的时候删除了数据
	assert.Equal(t, 0, len(keys))

}

//Merge的过程中有新数据的写入和删除

func TestDB_Merge5(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0 //不设置失效的阈值
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup) //协调多个goruntine执行
	wg.Add(1)                 //计数器+1,表示当前有一个goruntine要执行
	go func() {
		defer wg.Done() //表示当前goruntine执行未成
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait() //阻塞主线程，等待所有的goruntine执行完成
	//重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))
	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}
