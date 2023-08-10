package data

import "BitcaskDB/fio"

const DataFileSuffix = ".data"

//数据文件
type DataFile struct {
	FileId    uint32        //文件ID
	WriteOff  uint64        //文件写入到了哪个位置
	IoManager fio.IOManager //io的读写管理,使用接口，后期可以给多个使用
}

func OpenDataFile(dirPath string, fieldId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset uint64) (*LogRecord, uint64, error) {
	return nil, 0, nil
}
func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}
