package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	//一般情况
	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(logRecord)
	assert.NotNil(t, res1)
	//crc+type=5
	assert.Greater(t, n1, uint64(5))

	//value为空的情况
	logRecord = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res1, n1 = EncodeLogRecord(logRecord)
	assert.NotNil(t, res1)
	//crc+type=5
	assert.Greater(t, n1, uint64(5))
	t.Log(res1)

	//类型为deleted
	logRecord = &LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  LogRecordDeleted,
	}
	res1, n1 = EncodeLogRecord(logRecord)
	assert.NotNil(t, res1)
	//crc+type=5
	assert.Greater(t, n1, uint64(5))
	t.Log(res1)

}

func TestDecodeLogRecordHeader(t *testing.T) {
	//一般情况
	header := []byte{134, 200, 121, 217, 0, 8, 8}
	res, size := DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(3648637062), res.crc)
	assert.Equal(t, LogRecordNormal, res.recordType)
	assert.Equal(t, uint32(4), res.keySize)
	assert.Equal(t, uint32(4), res.valueSize)

	//value为空的情况
	header = []byte{189, 247, 47, 168, 0, 8, 0}
	res, size = DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(2821715901), res.crc)
	assert.Equal(t, LogRecordNormal, res.recordType)
	assert.Equal(t, uint32(4), res.keySize)
	assert.Equal(t, uint32(0), res.valueSize)

	//deleted情况
	header = []byte{135, 174, 155, 64, 1, 8, 8}
	res, size = DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(1083944583), res.crc)
	assert.Equal(t, LogRecordDeleted, res.recordType)
	assert.Equal(t, uint32(4), res.keySize)
	assert.Equal(t, uint32(4), res.valueSize)

}

func TestGetLogRecordCRC(t *testing.T) {
	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  LogRecordNormal,
	}
	header := []byte{134, 200, 121, 217, 0, 8, 8}
	crc := getLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(0x18f71746), crc)

	logRecord = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	header = []byte{189, 247, 47, 168, 0, 8, 0}
	crc = getLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(0xe58fc09), crc)

	logRecord = &LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  LogRecordDeleted,
	}
	header = []byte{135, 174, 155, 64, 1, 8, 8}
	crc = getLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(0xd979c886), crc)

}
