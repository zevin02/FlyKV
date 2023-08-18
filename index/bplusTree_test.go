package index

import (
	"BitcaskDB/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

const DirPath = "/home/zevin/githubmanage/program/BitcaskDB/tmp"

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(DirPath, "bptree-put")
	os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(DirPath)
	}()
	tree := NewBPT(path, false)
	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{123, 9999})
	assert.Nil(t, res1)
	tree.Put([]byte("abc"), &data.LogRecordPos{123, 9999})
	res2 := tree.Put([]byte("aac"), &data.LogRecordPos{123, 99})
	assert.Equal(t, uint32(123), res2.Fid)
	assert.Equal(t, uint64(9999), res2.Offset)

}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(DirPath, "bptree-get")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(DirPath)
	}()
	tree := NewBPT(path, false)
	pos := tree.Get([]byte("not-exist"))
	assert.Nil(t, pos)
	tree.Put([]byte("aac"), &data.LogRecordPos{123, 9999})
	tree.Put([]byte("abc"), &data.LogRecordPos{1231, 9999})
	tree.Put([]byte("acc"), &data.LogRecordPos{1232, 9999})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)
	tree.Put([]byte("aac"), &data.LogRecordPos{123, 99992})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)

}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(DirPath, "bptree-del")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(DirPath)
	}()
	tree := NewBPT(path, false)
	res1, ok1 := tree.Delete([]byte("no-exist"))
	assert.False(t, ok1)
	assert.Nil(t, res1)
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 9999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1231, Offset: 9999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1232, Offset: 9999})
	res2, ok2 := tree.Delete([]byte("aac"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(123), res2.Fid)
	assert.Equal(t, uint64(9999), res2.Offset)
	assert.Nil(t, tree.Get([]byte("aac")))
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(DirPath, "bptree-size")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(DirPath)
	}()

	tree := NewBPT(path, false)
	assert.Equal(t, 0, tree.Size())
	tree.Put([]byte("aac"), &data.LogRecordPos{123, 9999})
	tree.Put([]byte("abc"), &data.LogRecordPos{1231, 9999})
	tree.Put([]byte("acc"), &data.LogRecordPos{1232, 9999})
	assert.Equal(t, 3, tree.Size())
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(DirPath, "bptree-iter")
	os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(DirPath)
	}()
	tree := NewBPT(path, false)
	tree.Put([]byte("aac"), &data.LogRecordPos{123, 9999})
	tree.Put([]byte("abc"), &data.LogRecordPos{123, 9999})
	tree.Put([]byte("acc"), &data.LogRecordPos{123, 9999})
	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
	iter.Seek([]byte("aa"))
	assert.NotNil(t, iter.Key())
	assert.NotNil(t, iter.Value())
}
