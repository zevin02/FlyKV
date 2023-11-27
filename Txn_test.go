package FlexDB

import (
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPut(t *testing.T) {
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

	//测试读取视图
	txn1.Commit()
	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, expectVal2, val)
}
