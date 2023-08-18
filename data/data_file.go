package data

import (
	"BitcaskDB/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	DataFileSuffix        = ".data"
	HintFileName          = "hint-index" //里面存储的都是索引信息
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

var (
	ErrInvalidCrc = errors.New("invalid crc value,log record maybe error")
)

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        //文件ID
	WriteOff  uint64        //文件写入到了哪个位置
	IoManager fio.IOManager //管理io的读写管理,使用接口，
}

//OpenDataFile 打开新的数据文件，作为一个新的活跃文件
func OpenDataFile(dirPath string, fileId uint32, managerType fio.IOManagerType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, managerType)
}

// OpenHintFile 打开一个hint文件在merge的时候
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StanderFIO)
}

// OpenMergeFinishedFile 打开一个merge完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StanderFIO)
}

// OpenSeqNoFile 打开一个merge完成的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StanderFIO)
}

//生成datafile文件
func newDataFile(fileName string, fileId uint32, ioType fio.IOManagerType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

func GetDataFileName(dirPath string, fileId uint32) string {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, DataFileSuffix))
	return fileName
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
	if header.Crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}
	//取出key和value的长度
	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize)
	var recordSize = headerSize + keySize + valueSize //当前记录的字节长度
	logRecord := &LogRecord{Type: header.RecordType}
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
	crc := GetLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.Crc {
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

// WriteHintRecord 将位置索引信息写入到hint文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	//value是该key的位置信息
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
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

func (df *DataFile) SetIOManager(dirPath string, ioType fio.IOManagerType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	IOmanager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = IOmanager
	return nil
}
