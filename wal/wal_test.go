package wal

import (
	"FlexDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

//测试full ,fist+last
func TestWal_Write1(t *testing.T) {
	opts := defaultOpt
	wal, err := Open(opts)
	defer destroyFile(opts.dirPath)
	assert.Nil(t, err)
	assert.NotNil(t, wal)
	//测试full
	pos, err := wal.Write([]byte("asd"))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	//测试first+last
	pos, err = wal.Write([]byte("bcdef"))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), pos.blockID)
	assert.Equal(t, uint32(19), pos.chunkSize)
	assert.Equal(t, uint32(10), pos.chunkOffset)
	assert.Equal(t, uint32(0), pos.segmentID)
	assert.Equal(t, uint32(29), wal.currSegOffset)
	assert.Equal(t, uint32(9), wal.currBlcokOffset)
	assert.Equal(t, uint32(1), wal.BlockId)
	assert.Equal(t, uint32(0), wal.segmentID)
}

//
func TestWal_Write2(t *testing.T) {
	opts := defaultOpt
	wal, err := Open(opts)
	defer destroyFile(opts.dirPath)
	assert.Nil(t, err)
	assert.NotNil(t, wal)
	//测试full
	pos, err := wal.Write(utils.RandomValue(8))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	pos, err = wal.Write(utils.RandomValue(2))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	//测试first+last
	pos, err = wal.Write(utils.RandomValue(22))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), pos.blockID)
	assert.Equal(t, uint32(43), pos.chunkSize)
	assert.Equal(t, uint32(9), pos.chunkOffset)
	assert.Equal(t, uint32(0), pos.segmentID)
	assert.Equal(t, uint32(12), wal.currSegOffset)
	assert.Equal(t, uint32(12), wal.currBlcokOffset)
	assert.Equal(t, uint32(3), wal.BlockId)
	assert.Equal(t, uint32(1), wal.segmentID)
}
