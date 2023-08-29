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
	opts.BlockSize = 20
	opts.SegmentSize = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentSize
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

//测试full+padding +full+first+midlle+last
func TestWal_Write2(t *testing.T) {
	opts := defaultOpt
	opts.BlockSize = 20
	opts.SegmentSize = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentSize
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

//测试full+full+full+padding+first+last+paddding+first+middle+last
func TestWal_Write3(t *testing.T) {
	opts := defaultOpt
	opts.BlockSize = 20
	opts.SegmentSize = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentSize
	wal, err := Open(opts)
	defer destroyFile(opts.dirPath)
	assert.Nil(t, err)
	assert.NotNil(t, wal)
	//测试full
	pos, err := wal.Write(utils.RandomValue(2))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	//测试full
	pos, err = wal.Write(utils.RandomValue(4))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	//测试full

	pos, err = wal.Write(utils.RandomValue(12))
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	//padding+first+last
	pos, err = wal.Write(utils.RandomValue(19))
	//padding+first+middle+last
	pos, err = wal.Write(utils.RandomValue(27))

	assert.Equal(t, uint32(4), pos.blockID)
	assert.Equal(t, uint32(48), pos.chunkSize)
	assert.Equal(t, uint32(0), pos.chunkOffset)
	assert.Equal(t, uint32(1), pos.segmentID)
	assert.Equal(t, uint32(8), wal.currSegOffset)
	assert.Equal(t, uint32(8), wal.currBlcokOffset)
	assert.Equal(t, uint32(6), wal.BlockId)
	assert.Equal(t, uint32(2), wal.segmentID)
}

func TestWal_Read1(t *testing.T) {
	opts := defaultOpt
	opts.BlockSize = 20
	opts.SegmentSize = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentSize
	wal, err := Open(opts)
	defer destroyFile(opts.dirPath)
	assert.Nil(t, err)
	assert.NotNil(t, wal)
	//测试full
	val1 := utils.RandomValue(2)
	pos1, err := wal.Write(val1)
	assert.NotNil(t, pos1)
	assert.Nil(t, err)

	res, err := wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, val1, res)
	//测试fulll
	val := utils.RandomValue(4)

	pos, err := wal.Write(val)
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	res, err = wal.Read(pos)
	assert.Nil(t, err)
	assert.Equal(t, val, res)
	//测试full
	val = utils.RandomValue(12)

	pos, err = wal.Write(val)
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	res, err = wal.Read(pos)
	assert.Nil(t, err)
	assert.Equal(t, val, res)

	//padding+first+last
	val = utils.RandomValue(19)
	pos, err = wal.Write(val)
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	res, err = wal.Read(pos)
	assert.Nil(t, err)
	assert.Equal(t, val, res)

	////padding+first+middle+last
	val = utils.RandomValue(27)
	pos, err = wal.Write(val)
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	res, err = wal.Read(pos)
	assert.Nil(t, err)
	assert.Equal(t, val, res)

	//读取第一个文件中的缓存数据
	res, err = wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, val1, res)
	//
	//assert.Equal(t, uint32(4), pos.blockID)
	//assert.Equal(t, uint32(48), pos.chunkSize)
	//assert.Equal(t, uint32(0), pos.chunkOffset)
	//assert.Equal(t, uint32(1), pos.segmentID)
	//assert.Equal(t, uint32(8), wal.currSegOffset)
	//assert.Equal(t, uint32(8), wal.currBlcokOffset)
	//assert.Equal(t, uint32(6), wal.BlockId)
	//assert.Equal(t, uint32(2), wal.segmentID)
}
