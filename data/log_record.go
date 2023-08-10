package data

import "encoding/binary"

//LogRecordPos 数据在内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件ID，该数据存储在哪个文件中
	Offset uint64 //偏移，数据存储到数据文件的哪个位置
}

//将Logrecordtype等价于byte类型，增加可读性质
type LogRecordType = byte

const (
	// itoa 相当于0,往后进行枚举
	//LogRecordNormal：正常写入
	//Deleted:删除数据
	LogRecordNormal LogRecordType = itoa
	LogRecordDeleted
)

type LogRecordHeader struct {
	crc        uint32        //crc校验
	recordType LogRecordType //标识LogRecord的类型
	keySize    uint32        //key的长度
	valueSize  uint32        //value的长度
}

//crc type keysize valuesize
//4  +  1  +   5    +   5=15

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

//写入到数据文件的记录,数据文件是追加写入的，类似日志格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

//对LogRecord进行编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte, uint64) {
	return nil, 0
}

//对字节数组进行解码，得到头部数据信息
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64, error) {
	return nil, 0, nil
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
