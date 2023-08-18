package test

import (
	"BitcaskDB/data"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	//一般情况
	logRecord := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  data.LogRecordNormal,
	}
	res1, n1 := data.EncodeLogRecord(logRecord)
	assert.NotNil(t, res1)
	//crc+type=5
	assert.Greater(t, n1, uint64(5))

	//value为空的情况
	logRecord = &data.LogRecord{
		Key:  []byte("name"),
		Type: data.LogRecordNormal,
	}
	res1, n1 = data.EncodeLogRecord(logRecord)
	assert.NotNil(t, res1)
	//crc+type=5
	assert.Greater(t, n1, uint64(5))

	//类型为deleted
	logRecord = &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  data.LogRecordDeleted,
	}
	res1, n1 = data.EncodeLogRecord(logRecord)
	assert.NotNil(t, res1)
	//crc+type=5
	assert.Greater(t, n1, uint64(5))

}

func TestDecodeLogRecordHeader(t *testing.T) {
	//一般情况
	header := []byte{134, 200, 121, 217, 0, 8, 8}
	res, size := data.DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(3648637062), res.Crc)
	assert.Equal(t, data.LogRecordNormal, res.RecordType)
	assert.Equal(t, uint32(4), res.KeySize)
	assert.Equal(t, uint32(4), res.ValueSize)

	//value为空的情况
	header = []byte{189, 247, 47, 168, 0, 8, 0}
	res, size = data.DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(2821715901), res.Crc)
	assert.Equal(t, data.LogRecordNormal, res.RecordType)
	assert.Equal(t, uint32(4), res.KeySize)
	assert.Equal(t, uint32(0), res.ValueSize)

	//deleted情况
	header = []byte{135, 174, 155, 64, 1, 8, 8}
	res, size = data.DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(1083944583), res.Crc)
	assert.Equal(t, data.LogRecordDeleted, res.RecordType)
	assert.Equal(t, uint32(4), res.KeySize)
	assert.Equal(t, uint32(4), res.ValueSize)

}

func TestGetLogRecordCRC(t *testing.T) {
	logRecord := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  data.LogRecordNormal,
	}
	header := []byte{134, 200, 121, 217, 0, 8, 8}
	crc := data.GetLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(0x18f71746), crc)

	logRecord = &data.LogRecord{
		Key:  []byte("name"),
		Type: data.LogRecordNormal,
	}
	header = []byte{189, 247, 47, 168, 0, 8, 0}
	crc = data.GetLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(0xe58fc09), crc)

	logRecord = &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  data.LogRecordDeleted,
	}
	header = []byte{135, 174, 155, 64, 1, 8, 8}
	crc = data.GetLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(0xd979c886), crc)

}
