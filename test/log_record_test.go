package test

import (
	"FlexDB/data"
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
	header := []byte{34, 221, 28, 240, 0, 234, 192, 151, 44, 8, 8}
	res, size := data.DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(11), size)
	assert.Equal(t, uint32(4028423458), res.Crc)
	assert.Equal(t, data.LogRecordNormal, res.RecordType)
	assert.Equal(t, uint32(748142826), res.Tstamp)

	assert.Equal(t, uint32(8), res.KeySize)
	assert.Equal(t, uint32(8), res.ValueSize)

	//value为空的情况
	header = []byte{212, 153, 35, 211, 0, 244, 155, 155, 44, 8, 0}
	res, size = data.DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(11), size)
	assert.Equal(t, uint32(3542325716), res.Crc)
	assert.Equal(t, data.LogRecordNormal, res.RecordType)
	assert.Equal(t, uint32(8), res.KeySize)
	assert.Equal(t, uint32(0), res.ValueSize)
	assert.Equal(t, uint32(748395508), res.Tstamp)

	//deleted情况
	header = []byte{100, 64, 149, 155, 1, 103, 184, 159, 44, 8, 8}
	res, size = data.DecodeLogRecordHeader(header)
	assert.NotNil(t, res)
	assert.Equal(t, int64(11), size)
	assert.Equal(t, uint32(2610249828), res.Crc)
	assert.Equal(t, data.LogRecordDeleted, res.RecordType)
	assert.Equal(t, uint32(8), res.KeySize)
	assert.Equal(t, uint32(8), res.ValueSize)
	assert.Equal(t, uint32(748664935), res.Tstamp)

}

func TestGetLogRecordCRC(t *testing.T) {
	logRecord := &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  data.LogRecordNormal,
	}
	header := []byte{34, 221, 28, 240, 0, 234, 192, 151, 44, 8, 8}
	crc := data.GetLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(4028423458), crc)

	logRecord = &data.LogRecord{
		Key:  []byte("name"),
		Type: data.LogRecordNormal,
	}
	header = []byte{212, 153, 35, 211, 0, 244, 155, 155, 44, 8, 0}
	crc = data.GetLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(3542325716), crc)

	logRecord = &data.LogRecord{
		Key:   []byte("name"),
		Value: []byte("lily"),
		Type:  data.LogRecordDeleted,
	}
	header = []byte{100, 64, 149, 155, 1, 103, 184, 159, 44, 8, 8}
	crc = data.GetLogRecordCRC(logRecord, header[crc32.Size:])
	assert.Equal(t, uint32(2610249828), crc)

}
