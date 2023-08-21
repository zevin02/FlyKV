package FlexDB

import (
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOperations
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	//写数据后并没有提交
	wb := db.NewWriteBatch(DefaultWriteBatchOption)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	//正常提交数据后读取数据
	err = wb.Commit()
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	//删除有效数据
	wb1 := db.NewWriteBatch(DefaultWriteBatchOption)
	err = wb1.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb1.Commit()
	assert.Nil(t, err)

	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, val2)
}

//
func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOperations
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOption)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()

	assert.Nil(t, err)
	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10))
	err = wb.Commit()
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	//重启
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Equal(t, uint64(2), db2.seqNo)
}

//在commit之前中断掉，事务没有提交成功
func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOperations
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	_ = db.ListKeys()
	wbOpts := DefaultWriteBatchOption
	wbOpts.MaxWriteNum = 10000000
	wb := db.NewWriteBatch(wbOpts)
	for i := 0; i < 500000; i++ {
		err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)
}
