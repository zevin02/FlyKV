package mvcc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPut(t *testing.T) {
	ki := &KeyIndex{key: []byte("foo")}
	ki.put(2, 0)
	ki.put(3, 0)
	ki.put(4, 0)

	assert.Equal(t, Revision{4, 0}, ki.modified)
	assert.Equal(t, 3, len(ki.generations[0].revs))
	ki.Tombstone(5)
	// 删除当前的generation,并且生成一个最新的generation，供下一次操作
	assert.Equal(t, 2, len(ki.generations))
	assert.Equal(t, Revision{5, 0}, ki.modified)

}

func TestGet(t *testing.T) {
	ki := &KeyIndex{key: []byte("foo")}
	ki.put(2, 0)
	ki.put(3, 0)
	ki.put(4, 0)
	ki.Tombstone(5)
	// 删除当前的generation,并且生成一个最新的generation，供下一次操作
	assert.Equal(t, 2, len(ki.generations))

	rev := ki.get(3)
	assert.Equal(t, Revision{2, 0}, *rev)
	rev = ki.get(4)
	assert.Equal(t, Revision{3, 0}, *rev)
	rev = ki.get(5)
	assert.Equal(t, Revision{4, 0}, *rev)
	rev = ki.get(6)
	assert.Nil(t, rev)
	ki.put(7, 0)
	rev = ki.get(6)
	assert.Nil(t, rev)
	rev = ki.get(9)
	assert.Equal(t, Revision{7, 0}, *rev)
	ki.put(8, 0)
	ki.Tombstone(9)
	rev = ki.get(9)

}

//测试treeindex的正常使用
func TestTreeIndex1(t *testing.T) {
	ti := NewTreeIndex() //创建一个treeIndex对像
	ti.Put([]byte("foo"), Revision{1, 0})
	rev, err := ti.Get([]byte("foo"), 2)
	assert.Nil(t, err)
	assert.NotNil(t, rev)
	ti.Put([]byte("foo"), Revision{2, 0})
	ti.Put([]byte("foo"), Revision{3, 0})
	ti.Put([]byte("foo"), Revision{4, 0})
	rev, err = ti.Get([]byte("foo"), 3)
	assert.Nil(t, err)
	err = ti.Tombstone([]byte("foo"), Revision{5, 0})
	assert.Nil(t, err)
	ti.Put([]byte("foo"), Revision{6, 0})
	ti.Put([]byte("foo"), Revision{7, 0})
	ti.Put([]byte("foo"), Revision{8, 0})
	rev, err = ti.Get([]byte("foo"), 8)
	assert.Nil(t, err)
}

//进行正常的测试
func TestTreeIndex2(t *testing.T) {
	ti := NewTreeIndex() //创建一个treeIndex对像
	ti.Put([]byte("foo"), Revision{1, 0})
	rev, err := ti.Get([]byte("foo1"), 2)
	assert.Nil(t, rev)
	assert.Equal(t, ErrRevisionNotFound, err)
}
