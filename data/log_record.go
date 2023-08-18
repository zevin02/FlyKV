package data

import (
	"encoding/binary"
	"hash/crc32"
)

//LogRecordPos 数据在内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件ID，该数据存储在哪个文件中
	Offset uint64 //偏移，数据存储到数据文件的哪个位置
	Size   uint32 //该数据存储在磁盘中的大小
}

//将Logrecordtype等价于byte类型，增加可读性质
type LogRecordType = byte

const (
	// itoa 相当于0,往后进行枚举
	//LogRecordNormal：正常写入
	//LogRecordDeleted:删除数据
	//LogRecordTxnFinished :事务结束的标志

	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

type LogRecordHeader struct {
	Crc        uint32        //crc校验
	RecordType LogRecordType //标识LogRecord的类型
	KeySize    uint32        //key的长度
	ValueSize  uint32        //value的长度
}

//crc type keysize valuesize
//4  +  1  +   5    +   5=15最长大小

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

//写入到数据文件的记录,数据文件是追加写入的，类似日志格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

//TransactionRecord 暂存的事务相关数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

//对LogRecord进行编码,返回字节数组和字节数组的长度
//crc type keysize  valuesize  key       value
//4   1     max(5)    max(5)   变长        变长

func EncodeLogRecord(logRecord *LogRecord) ([]byte, uint64) {
	//初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	//先写入一个字节的类型,后面根据logrecord数据来计算crc校验
	header[4] = logRecord.Type
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	//index现在就是header的大小，可能会比最大的小

	//计算真实logrecord的大小
	var size uint64 = uint64(index + len(logRecord.Key) + len(logRecord.Value))
	encByteBuf := make([]byte, size)
	//将header拷贝过来
	copy(encByteBuf[:index], header[:index])
	copy(encByteBuf[index:], logRecord.Key)
	copy(encByteBuf[index+len(logRecord.Key):], logRecord.Value)
	crc := crc32.ChecksumIEEE(encByteBuf[4:])
	//写入crc校验值，按照小端的格式，保证数据的完整性,避免数据在传输或者存储过程中遭到损坏
	binary.LittleEndian.PutUint32(encByteBuf[:4], crc)

	return encByteBuf, size
}

// EncodeLogRecordPos 将位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen64+binary.MaxVarintLen32*2)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], int64(pos.Offset))
	index += binary.PutVarint(buf[index:], int64(pos.Size))

	return buf[:index]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: uint64(offset),
		Size:   uint32(size),
	}
}

// DecodeLogRecordHeader 传入头部的字节数组
//传入头部的信息，头部的字节大小
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		//crc4个字节都不够
		return nil, 0
	}
	header := &LogRecordHeader{
		Crc:        binary.LittleEndian.Uint32(buf[:4]),
		RecordType: buf[4],
	}
	var index = 5
	//分别解码获得keysize，和字节大小
	keySize, n := binary.Varint(buf[index:])

	index += n
	valueSize, n := binary.Varint(buf[index:])
	index += n
	header.KeySize = uint32(keySize)
	header.ValueSize = uint32(valueSize)

	return header, int64(index)
}

//传入的是除了crc的header头部字节数组

func GetLogRecordCRC(lr *LogRecord, headerBuf []byte) uint32 {
	if lr == nil {
		return 0
	}
	//先计算header的crc校验值
	crc := crc32.ChecksumIEEE(headerBuf[:])
	//根据key和value的数据来更新crc校验值
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
