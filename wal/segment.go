package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

type ChunkType = byte

const (
	Full   ChunkType = iota //当前的chunk可以容纳在一个block中
	First                   //当前的chunk无法容纳在一个block中间，当前的block存储该chunk的第一个部分数据
	Middle                  //前面First存储的一部分数据后面剩余的数据还是无法容纳在一个block中间
	Last                    //前面由first和middle截取完了的数据，剩余的数据完全能够容纳在一个block中
)

//ChunkPos 某一个chunk所在位置的位置信息Pos
type ChunkPos struct {
	segmentID   uint32 //存储所在的文件id
	blockID     uint32 //存储所在的block的id
	chunkOffset uint32 //在所在block中的位置偏移
	chunkSize   uint32 //该chunk的大小
}

//Chunk的格式
//CRC     +     length    +   type   +   payload
//4       +       2       +    1     +     n
//header的长度是4+2+1=7字节的大小
const (
	headerSize                = 7      //header的固定大小
	BlockSize          uint32 = 20     //一个block固定是32KB
	SegmentMaxBlockNum uint32 = 3      //一个segment文件中最多可以存放多少个Block
	SegFileSuffix      string = ".seg" //所有seg文件都是".seg“后缀
	SegFilePerm               = 0644
)

//Segment 某一个具体的segment文件的信息
type Segment struct {
	fd *os.File //文件描述符句柄
}

//OpenSegment 打开一个新的segment文件
func OpenSegment(dirPath string, id uint32) (*Segment, error) {
	//打开一个文件
	fd, err := os.OpenFile(
		GetSegmentFile(dirPath, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		SegFilePerm,
	)
	seg := &Segment{
		fd: fd,
	}
	return seg, err
}

//GetSegmentFile 获得当前seg文件的文件名字
func GetSegmentFile(dirPath string, fileId uint32) string {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, SegFileSuffix))
	return fileName
}

//append
func (seg *Segment) append(buf []byte) {
	seg.fd.Write(buf)
}

func (seg *Segment) Size() (uint32, error) {
	stat, err := seg.fd.Stat()
	if err != nil {
		return 0, err
	}
	return uint32(stat.Size()), nil
}
