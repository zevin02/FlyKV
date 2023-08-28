package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Wal struct {
	currBlcokOffset uint32              //当前指向的block中的偏移大小
	currSegOffset   uint32              //当前segment文件中的偏移量
	segmentID       uint32              //当前指向的文件ID
	BlockId         uint32              //当前处理到的blockId
	activeFile      *Segment            //当前指向的活跃的segment文件
	olderFile       map[uint32]*Segment //当前文件已经达到阈值之后就开辟一个新的文件来进行处理
	mu              *sync.RWMutex       //当前Wal持有的读写锁
	option          Option              //当前的wal的配置项
	//cache           *lru.Cache          //缓存block数据,key是blockId，value是一个block大小的缓存
}

type Option struct {
	dirPath            string //所在的路经名
	BlockSize          uint32 //一个block固定是32KB
	SegmentMaxBlockNum uint32 //一个segment文件中最多可以存放多少个Block
	SegmentSize        uint32 //一个segment文件最大可以最大的大小

}

var defaultOpt = Option{
	dirPath:            "/home/zevin/tmp",
	BlockSize:          20,
	SegmentMaxBlockNum: 3,
	SegmentSize:        BlockSize * SegmentMaxBlockNum,
}

//Open 打开一个Wal实例
func Open(options Option) (*Wal, error) {
	//检查当前目录是否存在，如果不存在的话就需要创建
	if _, err := os.Stat(options.dirPath); os.IsNotExist(err) {
		//创建目录
		if err := os.MkdirAll(options.dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	wal := &Wal{
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*Segment),
		option:    options,
	}
	//读取当前目录下的所有.seg文件
	dirEntries, err := os.ReadDir(options.dirPath)
	if err != nil {
		return nil, err
	}
	var fileIds []int
	//遍历目录中的所有文件,找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), SegFileSuffix) {
			//对00001.data文件进行分割，拿到他的第一个部分00001

			trimmedName := strings.TrimLeft(entry.Name()[:len(entry.Name())-len(".seg")], "0") //去掉前导0
			// 转换为文件ID
			if trimmedName == "" {
				trimmedName = "0"
			}
			//获得文件ID
			fileId, err := strconv.Atoi(trimmedName) //获得文件ID
			if err != nil {
				return nil, err
			}
			fileIds = append(fileIds, fileId)
		}
	}
	//对文件ID进行排序，从小到大
	sort.Ints(fileIds)
	//遍历每个文件ID，打开对应的文件
	var i int = 0
	for i, fid := range fileIds {
		segFile, err := OpenSegment(options.dirPath, uint32(fid))
		if err != nil {
			return nil, err
		}
		if i == len(fileIds)-1 {
			//说明这个是最后一个id，就设置成活跃文件
			wal.activeFile = segFile
			wal.segmentID = uint32(fid)
		} else {
			//否则就放入到旧文件集合中
			wal.olderFile[uint32(fid)] = segFile
		}
	}
	blockID := uint32(i) * SegmentMaxBlockNum
	if wal.activeFile != nil {
		activeSize, err := wal.activeFile.Size()
		if err != nil {
			return nil, err
		}
		blockID += activeSize / BlockSize //这个问题
		wal.currSegOffset = activeSize

		wal.BlockId = blockID
	}

	return wal, nil
}

//Write 写入一个buf数据,并且返回具体写入的位置信息
func (wal *Wal) Write(data []byte) (*ChunkPos, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if wal.activeFile == nil {
		//当前没有active文件，就需要新创建一个
		segfile, err := OpenSegment(wal.option.dirPath, wal.segmentID)

		if err != nil {
			return nil, nil
		}
		wal.activeFile = segfile
	}
	length := len(data) //获得当前数据的长度

	//data的数据不能超过一个segmentSize大小，超过的话，直接报错
	if uint32(length) >= wal.option.SegmentSize {
		return nil, ErrPayloadExceedSeg
	}

	if headerSize+wal.currBlcokOffset >= BlockSize {
		//当前的block中连头部都添加不进去
		//填充一些无用的数据
		buf := make([]byte, BlockSize-wal.currSegOffset)
		wal.activeFile.append(buf)
		wal.BlockId++
		byteAdd := BlockSize - wal.currSegOffset
		wal.currSegOffset += byteAdd
		wal.currBlcokOffset = 0
	}

	pos := &ChunkPos{
		segmentID:   wal.segmentID,
		blockID:     wal.BlockId,
		chunkOffset: wal.currBlcokOffset,
	}
	if uint32(length)+headerSize+wal.currBlcokOffset <= BlockSize {
		//如果当前数据长度+头部数据+当前block中的偏移量小于一个block大小，就可以直接放进去
		//把数据编码，并写入
		encBuf := encode(data, Full)
		wal.activeFile.append(encBuf)
		pos.chunkSize = uint32(len(encBuf))
		//更新偏移量信息
		wal.currBlcokOffset += uint32(len(encBuf))
		wal.currSegOffset += uint32(len(encBuf))
		return pos, nil
	}
	//如果走到这，说明当前的block无法容纳下该data，说明就需要将当前的data分在多个block中间存储
	var begin uint32 = 0 //两个指针指向要截取的数据的位置信息
	var end = uint32(length)
	times := 0 //循环了多少次，多进行一次循环就多7字节
	for {
		if begin == end {
			break
		}
		if wal.currSegOffset+headerSize >= wal.option.SegmentSize {
			//如果当前文件剩余的空间连头部数据都写不进去，就需要新开辟一个文件，因为数据最多不会超过一个文件的大小，所以这边检查一下文件大小
			//将数据进行持久化到磁盘中
			//如果是因为文件满了，就不需要添加padding数据
			if err := wal.Sync(); err != nil {
				return nil, err
			}
			//设置进旧文件集合中
			wal.olderFile[wal.segmentID] = wal.activeFile
			//新打开一个segment文件

			wal.segmentID += 1

			segfile, err := OpenSegment(wal.option.dirPath, wal.segmentID)

			if err != nil {
				return nil, nil
			}
			wal.activeFile = segfile

			wal.currSegOffset = 0   //把当前segment文件的指针设置成0
			wal.currBlcokOffset = 0 //把当前block偏移置为0
		}
		if begin == 0 {
			//说明当前数据还没有被切割
			//说明这个chunk就是First
			byteadd := BlockSize - wal.currBlcokOffset - headerSize
			encBuf := encode(data[begin:begin+byteadd], First)
			wal.activeFile.append(encBuf)
			begin += byteadd //更新begin指针的位置
			wal.currBlcokOffset = 0
			wal.BlockId++
			wal.currSegOffset += uint32(len(encBuf))

		} else {
			if end-begin+headerSize >= BlockSize {
				//剩余的还是超过了一个block大小
				encBuf := encode(data[begin:begin+BlockSize-headerSize], Middle)
				wal.activeFile.append(encBuf)
				begin += (BlockSize - headerSize)
				wal.BlockId++
				wal.currBlcokOffset = 0
				wal.currSegOffset += uint32(len(encBuf))

			} else {
				//剩余的小于一个block大小，
				encBuf := encode(data[begin:end], Last)
				wal.activeFile.append(encBuf)
				begin = end
				wal.currBlcokOffset += uint32(len(encBuf))
				wal.currSegOffset += uint32(len(encBuf))
			}
		}
		times++
	}
	pos.chunkSize = uint32(length + times*headerSize)

	return pos, nil
}

//Read 根据Pos位置来读取数据
func (wal *Wal) Read(pos ChunkPos) ([]byte, error) {
	return nil, nil
}

//Sync 将当前的活跃文件进行持久化
func (wal *Wal) Sync() error {
	return wal.activeFile.fd.Sync()
}

//将数据进行编码,编码出一个chunk出来
//Chunk的格式
//CRC     +     length    +   type   +   payload
//4       +       2       +    1     +     n
func encode(data []byte, chunkType ChunkType) []byte {
	encBuf := make([]byte, headerSize+len(data)) //开辟要返回的字节数组出来，返回
	//写入长度
	encBuf[6] = chunkType
	binary.LittleEndian.PutUint16(encBuf[4:], uint16(len(data))) //写入对应的data大小
	copy(encBuf[7:], data)
	//计算校验值
	crc := crc32.ChecksumIEEE(encBuf[:4])
	binary.LittleEndian.PutUint32(encBuf[:4], uint32(crc))
	return encBuf
}
