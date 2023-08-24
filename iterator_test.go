package FlexDB

import (
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

//迭代器使用完需要关闭掉
func TestDB_NewIterator_One_Value(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()
	assert.NotNil(t, iter)
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, utils.GetTestKey(10), iter.Key())
	assert.Equal(t, utils.GetTestKey(10), iter.Value())

}

func TestDB_NewIterator_Multi_Values(t *testing.T) {
	opts := DefaultOperations
	opts.DirPath = DirPath
	opts.IndexType = BPT
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	err = db.Put([]byte("annde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("accde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ssdde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("anade"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ssnde"), utils.RandomValue(10))
	err = db.Put([]byte("bbcde"), utils.RandomValue(10))
	err = db.Put([]byte("sfqde"), utils.RandomValue(10))
	err = db.Put([]byte("ssfde"), utils.RandomValue(10))
	err = db.Put([]byte("sgrde"), utils.RandomValue(10))
	err = db.Put([]byte("spvde"), utils.RandomValue(10))
	err = db.Put([]byte("aerde"), utils.RandomValue(10))
	err = db.Put([]byte("encodkey:11111"), utils.RandomValue(10))
	err = db.Put([]byte("encodkey:11314"), utils.RandomValue(10))
	err = db.Put([]byte("encodkey:111asd"), utils.RandomValue(10))
	err = db.Put([]byte("encodkey:1111fds1"), utils.RandomValue(10))
	err = db.Put([]byte("encodkey:11gewgw111"), utils.RandomValue(10))
	assert.Nil(t, err)
	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()
	assert.NotNil(t, iter)
	//正序遍历
	t.Log("========1=========")
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		t.Log(string(iter.Key()))

	}
	t.Log("========2=========")
	//重置之后，从比"s“大的位置开始正序遍历
	iter.Rewind()
	for iter.Seek([]byte("s")); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		t.Log(string(iter.Key()))
	}

	//重置之后，交替使用seek和next来迭代
	t.Log("========3=========")
	iter.Rewind()
	assert.Equal(t, db.options.indexNum, iter.iters.Len())

	iter.Seek([]byte("s"))
	assert.Equal(t, []byte("sfqde"), iter.Key())
	t.Log(string(iter.Key()))
	iter.Next()
	iter.Seek([]byte("s"))
	assert.Equal(t, []byte("sgrde"), iter.Key())
	t.Log(string(iter.Key()))
	iter.Next()
	iter.Seek([]byte("s"))
	assert.Equal(t, []byte("spvde"), iter.Key())

	//重置重新再迭代
	t.Log("========4=========")
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		t.Log(string(iter.Key()))
	}
	t.Log("========5=========")

	//反向迭代
	opts1 := DefaultIteratorOptions
	opts1.Reverse = false
	opts1.Prefix = []byte("encodkey:")
	iter1 := db.NewIterator(opts1)
	defer iter1.Close()
	assert.NotNil(t, iter)
	//设置了前缀的时候，初始化的都是大于这个前缀的值
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
		t.Log(string(iter1.Key()))
	}

}
