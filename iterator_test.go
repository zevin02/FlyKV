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
	assert.Nil(t, err)
	iter := db.NewIterator(DefaultIteratorOptions)
	defer iter.Close()
	assert.NotNil(t, iter)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		t.Log(string(iter.Key()))

	}
	iter.Rewind()
	for iter.Seek([]byte("s")); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		t.Log(string(iter.Key()))
	}
	t.Log("=================")
	iter.Rewind()
	iter.Seek([]byte("s"))
	t.Log(string(iter.Key()))
	iter.Next()
	iter.Seek([]byte("s"))
	t.Log(string(iter.Key()))
	t.Log("=================")
	//反向迭代
	opts1 := DefaultIteratorOptions
	opts1.Reverse = true
	opts1.Prefix = []byte("ss")
	iter1 := db.NewIterator(opts1)
	defer iter.Close()
	assert.NotNil(t, iter)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
		t.Log(string(iter1.Key()))
	}

}
