package index

import (
	"BitcaskDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()
	//插入一个边界数据
	res := bt.Put(nil, &data.LogRecordPos{1, 100})

	assert.True(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.True(t, res2)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()
	//插入一个边界数据
	res := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res)
	//测试key=nil获得相应的数据
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, uint64(100), pos1.Offset)

	//测试对一个key的重复使用获得的数据
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{1, 3})
	assert.True(t, res3)
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, uint64(3), pos2.Offset)

}

func TestBTree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)
	//删除一个nil对象
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{22, 33})
	assert.True(t, res3)
	//删除一个aaa对象
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)

}
