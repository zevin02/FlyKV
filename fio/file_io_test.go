package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	//在当前目录的tmp下面构造一个文件
	path := filepath.Join("/home/zevin/githubmanage/program/BitcaskDB/tmp", "a.txt")
	fio, err := newFileIOManager(path)
	//测试完成后将文件删除
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestNewFileIO_Write(t *testing.T) {
	//在当前目录的tmp下面构造一个文件
	path := filepath.Join("/home/zevin/githubmanage/program/BitcaskDB/tmp", "a.txt")
	fio, err := newFileIOManager(path)
	//测试完成后将文件删除
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("as"))
	assert.Equal(t, 2, n)
	n, err = fio.Write([]byte("asasdc"))
	assert.Equal(t, 6, n)

}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/home/zevin/githubmanage/program/BitcaskDB/tmp", "a.txt")
	fio, err := newFileIOManager(path)
	//测试完成后将文件删除
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/home/zevin/githubmanage/program/BitcaskDB/tmp", "a.txt")
	fio, err := newFileIOManager(path)
	//测试完成后将文件删除
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	err = fio.Sync()
	assert.Nil(t, err)

}
func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/home/zevin/githubmanage/program/BitcaskDB/tmp", "a.txt")
	fio, err := newFileIOManager(path)
	//测试完成后将文件删除
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	err = fio.Close()
	assert.Nil(t, err)

}
