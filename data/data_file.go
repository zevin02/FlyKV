package data

import (
	"BitcaskDB/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileSuffix = ".data"

var (
	ErrInvalidCrc = errors.New("invalid crc value,log record maybe error")
)

//数据文件
type DataFile struct {
	FileId    uint32        //文件ID
	WriteOff  uint64        //文件写入到了哪个位置
	IoManager fio.IOManager //管理io的读写管理,使用接口，
}

//OpenDataFile 打开新的数据文件，作为一个新的活跃文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, DataFileSuffix))
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

//ReadLogRecord 根据offset从数据文件中读取LogRecord
//返回的第一个参数是读取的这个日志信息
//返回的第二个参数是该日志的长度
func (df *DataFile) ReadLogRecord(offset uint64) (*LogRecord, uint64, error) {
	//读取文件的时候，需要先获得整个文件的大小，避免读取删除logrecord的时候，整个记录的大小小于headersize
	filesize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	//如果大小
	var headerBytes int64 = maxLogRecordHeaderSize
	if int64(offset)+headerBytes > filesize {
		//超过了文件上限，就读到文件结尾即可
		headerBytes = filesize - int64(offset)
	}

	// 读取header信息
	headerBuf, err := df.readNByte(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	//对头部进行解码
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	if header == nil {
		//头部为空，没有读取到，就说明这个文件为空，或者已经读取完了
		return nil, 0, io.EOF
	}
	//同样也是空数据
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	//取出key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize //当前记录的字节长度
	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNByte(keySize+valueSize, offset+uint64(headerSize))
		if err != nil {
			return nil, 0, nil
		}
		//解除key和value
		//[low:high]左边是起始的索引位置，右边是结束的索引位置，不包含
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	//数据的crc是否正确，检查有效性,从第4个字节开始进行校验
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		//校验检查的有问题
		return nil, 0, ErrInvalidCrc
	}
	//检验正确，有效数据进行返回
	return logRecord, uint64(recordSize), nil
}
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	nByte, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += uint64(nByte) //更新当前文件写到哪个位置了
	return nil
}

func (df *DataFile) readNByte(n int64, offset uint64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, int64(offset))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
