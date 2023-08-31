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
	opts := DefaultWalOpt
	opts.BlockSize = 20
	opts.SegmentMaxBlockNum = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentMaxBlockNum
	wal, err := Open(opts)
	defer destroyFile(opts.DirPath)
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
	_, chunkPosArr, err := wal.GetAllChunkInfo()
	//当一个data包含在不同的block的时候，会出现问题
	assert.Equal(t, 2, len(chunkPosArr))
	assert.Nil(t, err)
}

//测试full+padding +full+first+midlle+last
func TestWal_Write2(t *testing.T) {
	opts := DefaultWalOpt
	opts.BlockSize = 20
	opts.SegmentMaxBlockNum = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentMaxBlockNum
	wal, err := Open(opts)
	defer destroyFile(opts.DirPath)
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
	pos, err = wal.Write(utils.RandomValue(22)) //
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
	_, chunkPosArr, err := wal.GetAllChunkInfo()
	assert.Equal(t, 3, len(chunkPosArr))
	assert.Nil(t, err)
}

//测试full+full+full+padding+first+last+paddding+first+middle+last
func TestWal_Write3(t *testing.T) {
	opts := DefaultWalOpt
	opts.BlockSize = 20
	opts.SegmentMaxBlockNum = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentMaxBlockNum
	wal, err := Open(opts)
	defer destroyFile(opts.DirPath)
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
	opts := DefaultWalOpt
	opts.BlockSize = 20
	opts.SegmentMaxBlockNum = 3
	opts.SegmentSize = opts.BlockSize * opts.SegmentMaxBlockNum
	wal, err := Open(opts)
	defer destroyFile(opts.DirPath)
	assert.Nil(t, err)
	assert.NotNil(t, wal)
	//测试full
	val1 := utils.RandomValue(2)
	pos1, err := wal.Write(val1)
	assert.NotNil(t, pos1)
	assert.Nil(t, err)

	res, nextChunkPos, err := wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, val1, res)
	assert.Equal(t, uint32(9), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(0), nextChunkPos.blockID)
	assert.Equal(t, uint32(0), nextChunkPos.segmentID)

	//测试fulll
	val := utils.RandomValue(4)

	pos, err := wal.Write(val)
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	res, nextChunkPos, err = wal.Read(nextChunkPos)
	assert.Nil(t, err)
	assert.Equal(t, val, res)
	assert.Equal(t, uint32(0), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(1), nextChunkPos.blockID)
	assert.Equal(t, uint32(0), nextChunkPos.segmentID)

	//测试full
	val = utils.RandomValue(12)

	pos, err = wal.Write(val)
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	res, nextChunkPos, err = wal.Read(nextChunkPos)
	assert.Nil(t, err)
	assert.Equal(t, val, res)
	assert.Equal(t, uint32(0), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(2), nextChunkPos.blockID)
	assert.Equal(t, uint32(0), nextChunkPos.segmentID)

	//padding+first+last
	val = utils.RandomValue(19)
	pos, err = wal.Write(val)
	assert.NotNil(t, pos)
	assert.Nil(t, err)
	res, nextChunkPos, err = wal.Read(pos)
	assert.Nil(t, err)
	assert.Equal(t, val, res)
	assert.Equal(t, uint32(0), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(4), nextChunkPos.blockID)
	assert.Equal(t, uint32(1), nextChunkPos.segmentID)

	////padding+first+middle+last
	val5 := utils.RandomValue(27)
	pos2, err := wal.Write(val5)
	assert.NotNil(t, pos2)
	assert.Nil(t, err)
	res, nextChunkPos, err = wal.Read(pos2) //read中不能对外面的pos进行修改
	assert.Nil(t, err)
	assert.Equal(t, val5, res)
	assert.Equal(t, uint32(8), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(6), nextChunkPos.blockID)
	assert.Equal(t, uint32(2), nextChunkPos.segmentID)

	//读取第一个文件中的缓存数据
	res, nextChunkPos, err = wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, val1, res)
	//
	assert.Equal(t, uint32(9), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(0), nextChunkPos.blockID)
	assert.Equal(t, uint32(0), nextChunkPos.segmentID)

	assert.Equal(t, uint32(4), pos2.blockID)
	assert.Equal(t, uint32(48), pos2.chunkSize)
	assert.Equal(t, uint32(0), pos2.chunkOffset)
	assert.Equal(t, uint32(1), pos2.segmentID)
	assert.Equal(t, uint32(8), wal.currSegOffset)
	assert.Equal(t, uint32(8), wal.currBlcokOffset)
	assert.Equal(t, uint32(6), wal.BlockId)
	assert.Equal(t, uint32(2), wal.segmentID)

	//关闭再重启
	assert.Nil(t, wal.Close())
	wal1, err := Open(opts)
	assert.NotNil(t, wal1)
	assert.Equal(t, uint32(8), wal1.currSegOffset)
	assert.Equal(t, uint32(8), wal1.currBlcokOffset)
	assert.Equal(t, uint32(6), wal1.BlockId)
	assert.Equal(t, uint32(2), wal1.segmentID)
	//测试full
	val2 := utils.RandomValue(4)

	pos3, err := wal1.Write(val2)
	assert.NotNil(t, pos3)
	assert.Nil(t, err)
	ret, nextChunkPos, err := wal1.Read(pos3)
	assert.Nil(t, err)
	assert.Equal(t, val2, ret)
	assert.Equal(t, uint32(0), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(7), nextChunkPos.blockID)
	assert.Equal(t, uint32(2), nextChunkPos.segmentID)
	assert.Equal(t, uint32(19), wal1.currSegOffset)
	assert.Equal(t, uint32(19), wal1.currBlcokOffset)
	assert.Equal(t, uint32(6), wal1.BlockId)
	assert.Equal(t, uint32(2), wal1.segmentID)

	//测试segment2号文件
	res, nextChunkPos, err = wal1.Read(pos2) //read中不能对外面的pos进行修改
	assert.Nil(t, err)
	assert.Equal(t, val5, res)
	assert.Equal(t, uint32(8), nextChunkPos.chunkOffset)
	assert.Equal(t, uint32(6), nextChunkPos.blockID)
	assert.Equal(t, uint32(2), nextChunkPos.segmentID)

	assert.Nil(t, wal1.Close())

	//var chunkPosArray []*ChunkPos
	wal2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, wal2)
	//遍历获得所有的位置信息

	_, chunkPosArr, err := wal2.GetAllChunkInfo()
	assert.Equal(t, 6, len(chunkPosArr))
	assert.Nil(t, err)
}
