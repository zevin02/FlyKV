package test

import (
	"BitcaskDB/data"
	"BitcaskDB/fio"
	"github.com/stretchr/testify/assert"
	"testing"
)

//打开文件
func TestOpenDataFile(t *testing.T) {
	//打开一个数据活跃文件
	df1, err := data.OpenDataFile("/tmp", 0, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := data.OpenDataFile("/tmp", 1, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df2)
	//重复的打开同一个文件
	df3, err := data.OpenDataFile("/tmp", 1, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df3)

}

func TestDataFile_Write(t *testing.T) {

	//打开文件
	df1, err := data.OpenDataFile("/tmp", 0, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)
	//写入数据
	err = df1.Write([]byte("aa"))
	assert.Nil(t, err)
	err = df1.Write([]byte("bb"))
	assert.Nil(t, err)
	err = df1.Write([]byte("cc"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	df1, err := data.OpenDataFile("/tmp", 12, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	err = df1.Write([]byte("aa"))
	assert.Nil(t, err)
	err = df1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	df1, err := data.OpenDataFile("/tmp", 123, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	err = df1.Write([]byte("aa"))
	assert.Nil(t, err)
	err = df1.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	df1, err := data.OpenDataFile("/tmp", 1111, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)
	//只有一条logrecord
	logRecord := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("lilyai"),
		Type:  data.LogRecordNormal,
	}

	encBuf, size := data.EncodeLogRecord(logRecord)
	err = df1.Write(encBuf)
	assert.Nil(t, err)

	readRec, readSize, err := df1.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, logRecord, readRec)
	assert.Equal(t, size, readSize)

	//多条logrecord，从不同位置读取
	logRecord = &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
		Type:  data.LogRecordNormal,
	}

	encBuf, size = data.EncodeLogRecord(logRecord)
	err = df1.Write(encBuf)
	assert.Nil(t, err)

	readRec, readSize, err = df1.ReadLogRecord(17)
	assert.Nil(t, err)
	assert.Equal(t, logRecord, readRec)
	assert.Equal(t, size, readSize)

	//被删除的数据在文件的末尾
	logRecord = &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte(""),
		Type:  data.LogRecordDeleted,
	}

	encBuf, size = data.EncodeLogRecord(logRecord)
	err = df1.Write(encBuf)
	assert.Nil(t, err)

	readRec, readSize, err = df1.ReadLogRecord(39)
	assert.Nil(t, err)
	assert.Equal(t, logRecord, readRec)
	assert.Equal(t, size, readSize)

}
