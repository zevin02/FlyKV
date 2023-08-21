package test

import (
	"FlexDB/fio"
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap")
	//测试完成后将文件删除

	mio, err := fio.NewMMapIOManager(path)
	assert.Nil(t, err)
	//文件为空的情况
	b1 := make([]byte, 10)
	n, err := mio.Read(b1, 0)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	mio.Close()

	//有文件的内容
	f, err := fio.NewFileIOManager(path)
	//测试完成后将文件删除
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, f)
	f.Write([]byte("aaaaa"))
	f.Write([]byte("aaaaa"))

	mio1, err := fio.NewMMapIOManager(path)
	assert.Nil(t, err)
	size1, err := mio1.Size()

	assert.Equal(t, int64(10), size1)
	b1 = make([]byte, 10)
	n, err = mio1.Read(b1, 0)
	assert.Equal(t, 10, n)
	assert.Nil(t, err)
	mio1.Close()

}
