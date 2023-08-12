package BitcaskDB

import (
	"BitcaskDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

//测试完成之后销毁DB目录

func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()

		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOperations
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	//1.正常put一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	//2.重复put key相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	//3.key为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	//4.value为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	//5.写到数据文件进行了转化
	for i := 0; i < 1000000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFile))

	//6.重启后再Put数据
	err = db.Close()
	assert.Nil(t, err)

	//7.重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	//1.正常读取一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	//2.读取一个不存在的key
	val2, err := db.Get([]byte("unknow key"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	//3.值被重复put后读取
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	val3, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	//4.值被删除后再Get
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	val4, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Equal(t, 0, len(val4))

	//转化为了旧文件，从旧文件中读value
	for i := 100; i < 1000000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFile))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	//6.重启后，前面数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	//重启数据库
	db2, err := Open(opts)
	val6, err := db2.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(44))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	//1.正常删除一个存在的key
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(22))
	assert.Equal(t, ErrKeyNotFound, err)

	//2.删除一个不存在的key
	err = db.Delete([]byte("unknow key"))
	assert.Nil(t, err)

	//3.删除一个空的key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	//4.值被删除之后重新put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	//5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)
	//重启数据库
	db2, err := Open(opts)
	_, err = db2.Get(utils.GetTestKey(111))
	assert.Equal(t, ErrKeyNotFound, err)
	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	//数据库为空的情况
	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	//只有一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	//有多条数据的情况
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(111), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(1111), utils.RandomValue(20))
	assert.Nil(t, err)
	keys3 := db.ListKeys()
	for _, key := range keys3 {
		t.Log(string(key))
	}
	assert.Equal(t, 4, len(keys3))
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(111), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(1111), utils.RandomValue(20))
	assert.Nil(t, err)
	db.Fold(func(key []byte, value []byte) bool {
		//这里可以自定义对key和value进行操作
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)

}

func TestDB_Close(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
}
func TestDB_Sync(t *testing.T) {
	opts := DefaultOperations
	opts.FileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Sync()
	assert.Nil(t, err)
}
