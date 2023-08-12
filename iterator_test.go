package BitcaskDB

import (
	"BitcaskDB/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewIterator_One_Value(t *testing.T) {
	opts := DefaultOperations
	db, err := Open(opts)
	defer destroyDB(db)
	assert.NotNil(t, db)
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, utils.GetTestKey(10), iter.Key())
	assert.Equal(t, utils.GetTestKey(10), iter.Value())

}

func TestDB_NewIterator_Multi_Values(t *testing.T) {
	opts := DefaultOperations
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
	assert.Nil(t, err)
	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
	}
	iter.Rewind()
	for iter.Seek([]byte("a")); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
	//反向迭代
	opts1 := DefaultIteratorOptions
	opts1.Reverse = true
	opts1.Prefix = []byte("ss")
	iter1 := db.NewIterator(opts1)
	assert.NotNil(t, iter)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

}
