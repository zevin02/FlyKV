package FlexDB

import (
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

const DirPath = "/home/zevin/githubmanage/program/FlexDB/fortest"

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)
	//写数据后并没有提交
	wb := db.NewWriteBatch(DefaultWriteBatchOption, int64(1))
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10), 0)
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2), 1) //删除一个不存在的数据
	assert.NotNil(t, err)
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	err = wb.Delete(utils.GetTestKey(1), 2) //删除一个不存在的数据

	//正常提交数据后读取数据
	err = wb.Commit()
	assert.Nil(t, err)
	db.latestRevision = wb.beginRev + 1
	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	//删除有效数据
	wb1 := db.NewWriteBatch(DefaultWriteBatchOption, 3)
	err = wb1.Delete(utils.GetTestKey(1), 2)
	assert.Nil(t, err)
	err = wb1.Commit()
	assert.Nil(t, err)
	db.latestRevision = wb1.beginRev + 1
	val2, err := db.Get(utils.GetTestKey(1)) //删除之后，就不能读取之前的的数据了
	assert.Nil(t, val2)
}

//
func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOption, 1)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10), 0)
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1), 1)
	assert.Nil(t, err)

	err = wb.Commit()
	db.latestRevision = wb.beginRev + 1
	assert.Nil(t, err)
	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10), 2)
	err = wb.Commit()
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	//重启
	err = db.Close()
	db = nil
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
	opts.DirPath = DirPath
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	_ = db.ListKeys(DefaultIteratorOptions)
	wbOpts := DefaultWriteBatchOption
	wbOpts.MaxWriteNum = 10000000
	wb := db.NewWriteBatch(wbOpts, 2)
	for i := 0; i < 500000; i++ {
		err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024), int64(i))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)
}
