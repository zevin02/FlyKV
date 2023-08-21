package redis

import (
	"FlexDB"
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

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
