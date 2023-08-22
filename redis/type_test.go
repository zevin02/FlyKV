package redis

import (
	"FlexDB"
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

//================String=======================
func TestRedisDataStruct_Get(t *testing.T) {
	opts := FlexDB.DefaultOperations
	defer func() {
		os.RemoveAll(opts.DirPath)
	}()
	rds, err := NewRedisDataStruct(opts)

	assert.Nil(t, err)
	assert.Nil(t, rds.Set(utils.GetTestKey(1), 0, []byte("utils.RandomValue(100)")))
	rds.Set(utils.GetTestKey(2), 0, utils.RandomValue(100))
	rds.Set(utils.GetTestKey(3), time.Second*5, utils.RandomValue(100))
	val1, err := rds.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	val2, err := rds.Get(utils.GetTestKey(3))
	assert.NotNil(t, val2)
	_, err = rds.Get(utils.GetTestKey(4))
	assert.Equal(t, FlexDB.ErrKeyNotFound, err)
	rds.db.Close()
}

func TestRedisDataStruct_DEL(t *testing.T) {
	opts := FlexDB.DefaultOperations
	defer func() {
		os.RemoveAll(opts.DirPath)
	}()
	rds, err := NewRedisDataStruct(opts)
	assert.Nil(t, err)
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.Nil(t, rds.Set(utils.GetTestKey(11), 0, []byte("utils.RandomValue(100)")))
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)
	val1, err := rds.Get(utils.GetTestKey(11))
	assert.Nil(t, val1)
	assert.Equal(t, FlexDB.ErrKeyNotFound, err)
	rds.db.Close()
}

func TestRedisDataStruct_HGet(t *testing.T) {
	opts := FlexDB.DefaultOperations
	defer func() {
		os.RemoveAll(opts.DirPath)
	}()
	rds, err := NewRedisDataStruct(opts)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)
	v1 := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)
	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Equal(t, v1, val1)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.True(t, ok3)
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Equal(t, v2, val2)
}

func TestRedisDataStruct_HDel(t *testing.T) {
	opts := FlexDB.DefaultOperations
	defer func() {
		os.RemoveAll(opts.DirPath)
	}()
	rds, err := NewRedisDataStruct(opts)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)
	v1 := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)
	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Equal(t, v1, val1)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.True(t, ok3)
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Equal(t, v2, val2)
	assert.Nil(t, err)

	ok4, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.True(t, ok4)
	assert.Nil(t, err)
	//删除不存在的key

	ok5, err := rds.HDel(utils.GetTestKey(1), []byte("field4"))
	assert.False(t, ok5)
	assert.Nil(t, err)

	ok6, err := rds.HDel(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)

	assert.True(t, ok6)
}
