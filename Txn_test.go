package FlexDB

import (
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPut1(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	expectVal1 := utils.RandomValue(10)

	db.Put(utils.GetTestKey(2), expectVal1)
	txn1 := db.NewTXN(DefaultWriteBatchOption) //初始化一个事务
	err = txn1.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = txn1.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	expectVal2 := utils.RandomValue(10)
	//当前的是{0,2}
	err = txn1.Put(utils.GetTestKey(1), expectVal2)
	assert.Nil(t, err)
	val1, err := txn1.Get(utils.GetTestKey(1))
	assert.Equal(t, expectVal2, val1)
	val2, err := txn1.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, expectVal1, val2)

	db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	//在事务执行期间，外部db又存放数据
	val3, err := txn1.Get(utils.GetTestKey(2))
	assert.Equal(t, expectVal1, val3)
	//测试读取视图
	txn1.Commit()
	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, expectVal2, val)
}

//多个事务交替执行
func TestPut2(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	txn1 := db.NewTXN(DefaultWriteBatchOption) //初始化一个事务
	txn2 := db.NewTXN(DefaultWriteBatchOption) //初始化一个事务
	db.Put([]byte("1"), []byte("1"))
	txn2.Put([]byte("2"), []byte("2"))
	txn1.Put([]byte("3"), []byte("3"))
	val2, err := db.Get([]byte("2"))
	assert.Nil(t, val2)
	db.Put([]byte("4"), []byte("1"))

	val1, err := txn1.Get([]byte("4"))
	db.Get([]byte("4"))
	assert.NotNil(t, err)
	assert.Nil(t, val1)
}
