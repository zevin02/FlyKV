package index

import (
	"FlexDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res3)
	res4 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 112, Offset: 12})
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, uint64(12), res4.Offset)
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos1 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos1)
	pos2 := art.Get([]byte("key-noexist"))
	assert.Nil(t, pos2)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 111, Offset: 1223})
	pos3 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos3)
	assert.NotEqual(t, pos2, pos3)

}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	//删除一个不存在的key
	res1, ok1 := art.Delete([]byte("not-exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	//删除一个存在的key
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2, ok2 := art.Delete([]byte("key-1"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, uint64(12), res2.Offset)
	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)

}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 3, art.Size())
	//重复的数据也能去重
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 3, art.Size())

}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-11"), &data.LogRecordPos{Fid: 1, Offset: 12})
	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
	}

	iter.Seek([]byte("key-1"))
	assert.NotNil(t, iter.Key())
}
