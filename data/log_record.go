package data

//LogRecordPos 数据在内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件ID，该数据存储在哪个文件中
	Offset uint64 //偏移，数据存储到数据文件的哪个位置
}

//将Logrecordtype等价于byte类型，增加可读性质
type LogRecordType = byte

const (
	//itoa相当于0,往后进行枚举
	//Normal：正常写入
	//Deleted:删除数据
	LogRecordNormal LogRecordType = itoa
	LogRecordDeleted
)

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
