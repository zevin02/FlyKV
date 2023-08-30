package wal

import (
	"FlexDB/fio"
	"encoding/binary"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"hash/crc32"
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
	//fd        *os.File                   //文件描述符句柄
	IOManager fio.IOManager              //IO管理
	SegmentId uint32                     //标识当前的segmentId是多少
	blockId   uint32                     //标识但前的blockId是从哪里开始的
	cache     *lru.Cache[uint32, []byte] //读取缓存数据
}

//OpenSegment 打开一个新的segment文件
func (wal *Wal) OpenSegment(segmentId uint32, ioType fio.IOManagerType) (*Segment, error) {
	//打开一个文件

	ioManager, err := fio.NewIOManager(GetSegmentFile(wal.option.dirPath, wal.option.fileSuffix, segmentId), ioType)
	if err != nil {
		return nil, err
	}
	seg := &Segment{
		IOManager: ioManager,
		SegmentId: segmentId,
		blockId:   segmentId * SegmentMaxBlockNum,
		cache:     wal.cache,
	}
	return seg, err
}

//GetSegmentFile 获得当前seg文件的文件名字
func GetSegmentFile(dirPath, fileSuffix string, fileId uint32) string {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, fileSuffix))
	return fileName
}

//append
func (seg *Segment) append(buf []byte) (int, error) {
	return seg.IOManager.Write(buf)
}

func (seg *Segment) Size() (uint32, error) {
	size, err := seg.IOManager.Size()
	if err != nil {
		return 0, err
	}
	return uint32(size), nil

}

//ReadInternal 如果返回true，说明数据在当前的segment可以全部读取成功，否则就需要在开启第二个segment文件继续把数据读取完
func (seg *Segment) ReadInternal(blockId, chunkOffset uint32) (isComplete bool, numBlockRead uint32, data []byte, err error) {
	//由于一个semnet文件可以装segmentblocksize个block块
	//获得需要读取的blockID在当前的block块中是第几个block
	filesize, err := seg.Size()
	if err != nil {
		return false, 0, nil, err
	}
	curSegBlockId := blockId - seg.blockId //计算需要查找的blockId在当前的segment文件中是第几个block
	var (
		i        uint32 = 0
		res      []byte //用来返回的数据
		ok              = false
		begin           = chunkOffset
		readByte uint32 = BlockSize //需要读取多少个字节
	)

	for {
		//(curSegBlockId+i)*BlockSize=当前的segment文件中的某个block在segment文件中的偏移位置
		if curSegBlockId*BlockSize+begin+BlockSize > filesize {
			readByte = filesize - curSegBlockId*BlockSize
		}
		if readByte == 0 {
			//说明当前已经没有数据可以读取了，需要在下一个segment文件中继续读取数据
			break
		}

		blockBuf, err := seg.readBlock(curSegBlockId, readByte)
		if err != nil {
			return false, 0, nil, err
		}

		blockType, data, err := seg.readChunk(blockBuf, begin)
		if err != nil {
			return false, 0, nil, err
		}
		res = append(res, data...) //将data数据追加到res中
		if blockType == Full || blockType == Last {
			ok = true
			break
		} else {
			//说明他是middle或者first，那么当前数据读取完之后，还需要继续读取下一个block块
			begin = 0
		}
		i++
		curSegBlockId++
	}
	return ok, i, res, nil
}

//readBlock 读取一个block大小的数据
func (seg *Segment) readBlock(curSegBlockId, readByte uint32) ([]byte, error) {
	var (
		buf     []byte
		err     error
		ok      bool
		blockId = curSegBlockId + seg.SegmentId*SegmentMaxBlockNum
	)

	buf, ok = seg.cache.Get(GetCacheKey(seg.SegmentId, blockId)) //从LRU缓存中读取数据
	if !ok || uint32(len(buf)) < BlockSize {
		//缓存没有命中或者缓存中读取的数据小于一个block大小，也需要重新进行读取
		buf, err = seg.readNByte(readByte, BlockSize*curSegBlockId)
		if err != nil {
			return nil, err
		}
		seg.cache.Add(GetCacheKey(seg.SegmentId, blockId), buf) //将读取到的block数据添加到LRU中
	}
	return buf, nil
}

//readChunk 根据给定的chunkoffset从block中读取chunk数据
func (seg *Segment) readChunk(blockBuf []byte, begin uint32) (ChunkType, []byte, error) {
	//buf now is a blocksize buf
	header := blockBuf[begin : begin+headerSize] //提取出来头部信息
	blockType := header[6]
	length := binary.LittleEndian.Uint16(header[4:])
	crc := binary.LittleEndian.Uint32(header[:4]) //获得crc数据
	calCrc := crc32.ChecksumIEEE(header[4:])
	dataBegin := begin + headerSize
	dataEnd := begin + headerSize + uint32(length)
	data := blockBuf[dataBegin:dataEnd] //数据的数据
	calCrc = crc32.Update(calCrc, crc32.IEEETable, data)
	if calCrc != crc {
		return 0, nil, ErrInvalidCrc
	}
	return blockType, data, nil
}

//
func (seg *Segment) readNByte(n, offset uint32) (b []byte, err error) {
	b = make([]byte, n)
	_, err = seg.IOManager.Read(b, int64(offset))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func GetCacheKey(segmentID, blockID uint32) uint32 {
	key := (segmentID & 0xFFFF) | ((blockID & 0xFFFF) << 16)
	return key
}

func (seg *Segment) Sync() error {
	return seg.IOManager.Sync()
}

func (seg *Segment) Close() error {
	return seg.IOManager.Close()
}

func (seg *Segment) SetIOManager(dirPath, fileSuffix string, ioType fio.IOManagerType) error {
	if err := seg.IOManager.Close(); err != nil {
		return err
	}
	IOmanager, err := fio.NewIOManager(GetSegmentFile(dirPath, fileSuffix, seg.SegmentId), ioType)
	if err != nil {
		return err
	}
	seg.IOManager = IOmanager
	return nil
}
