package redis

import (
	"BitcaskDB"
	"BitcaskDB/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRedisDataStruct_Get(t *testing.T) {
	opts := BitcaskDB.DefaultOperations
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
	assert.Equal(t, BitcaskDB.ErrKeyNotFound, err)

}

func TestRedisDataStruct_DEL(t *testing.T) {
	opts := BitcaskDB.DefaultOperations
	rds, err := NewRedisDataStruct(opts)
	assert.Nil(t, err)
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.Nil(t, rds.Set(utils.GetTestKey(11), 0, []byte("utils.RandomValue(100)")))
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)
	val1, err := rds.Get(utils.GetTestKey(11))
	assert.Nil(t, val1)
	assert.Equal(t, BitcaskDB.ErrKeyNotFound, err)

}
