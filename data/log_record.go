package data

//LogRecordPos 数据在内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件ID，该数据存储在哪个文件中
	Offset uint64 //偏移，数据存储到数据文件的哪个位置
}
