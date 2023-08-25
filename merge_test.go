package FlexDB

import (
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"testing"
)

//没有任何数据的情况下进行merge
func TestDB_Merge(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)

	err = db.Merge(false)
	assert.Nil(t, err)
}

//全部都是有效数据
func TestDB_Merge2(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	opts.FileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0 //不设置失效的阈值
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge(false)
	assert.Nil(t, err)
	//重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	keys := db2.ListKeys(DefaultIteratorOptions)
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
	opts.DirPath = DirPath
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
		_, err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}
	err = db.Merge(false)
	assert.Nil(t, err)
	//重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)

	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	keys := db2.ListKeys(DefaultIteratorOptions)
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
	opts.DirPath = DirPath
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
		_, err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	err = db.Merge(false)
	assert.Nil(t, err)

	//重启校验
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)

	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	keys := db2.ListKeys(DefaultIteratorOptions)
	//Merge的时候删除了数据
	assert.Equal(t, 0, len(keys))

}

//Merge的过程中有新数据的写入和删除

func TestDB_Merge5(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
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
			_, err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge(false)
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
	keys := db2.ListKeys(DefaultIteratorOptions)
	assert.Equal(t, 10000, len(keys))
	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

//全部是无效的数据
func TestDB_Merge6(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
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
		_, err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	err = db.Merge(true)
	assert.Nil(t, err)
	keys := db.ListKeys(DefaultIteratorOptions)
	//Merge的时候删除了数据
	assert.Equal(t, 0, len(keys))

}

//有失败数据和被重复put的数据
func TestDB_Merge7(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
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
		_, err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}
	err = db.Merge(true)
	assert.Nil(t, err)

	keys := db.ListKeys(DefaultIteratorOptions)
	//Merge的时候删除了数据
	assert.Equal(t, 40000, len(keys))
	for i := 0; i < 10000; i++ {
		_, err := db.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 40000; i < 10000; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("new value in merge"), val)
	}
}

//全部都是有效数据
func TestDB_Merge8(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	opts.FileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0 //不设置失效的阈值
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge(true)
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(1024))
	assert.Nil(t, err)
	keys := db.ListKeys(DefaultIteratorOptions)
	assert.Equal(t, 50000, len(keys))
	for i := 0; i < 50000; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}

}

//Merge的过程中有新数据的写入和删除

func TestDB_Merge9(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	opts.FileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0 //不设置失效的阈值
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)

	wg := sync.WaitGroup{}
	m := sync.Map{}
	wg.Add(11)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				value := utils.RandomValue(5)
				m.Store(string(key), value)
				e := db.Put(key, value)
				assert.Nil(t, e)
			}
		}()
	}
	go func() {
		defer wg.Done()
		err = db.Merge(true)
		assert.Nil(t, err)
	}()
	wg.Wait()
	_, err = os.Stat(db.getMergePath())
	assert.Equal(t, true, os.IsNotExist(err))

	var count int
	m.Range(func(key, value any) bool {
		v, err := db.Get([]byte(key.(string)))
		assert.Nil(t, err)
		assert.Equal(t, value, v)
		count++
		return true
	})
	//assert.Equal(t, count, db.index.Size())
}
