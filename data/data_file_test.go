package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const DirPath = "/home/zevin/githubmanage/program/BitcaskDB/tmp"

//打开文件
func TestOpenDataFile(t *testing.T) {
	//打开一个数据活跃文件
	df1, err := OpenDataFile(DirPath, 0)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := OpenDataFile(DirPath, 1)
	assert.Nil(t, err)
	assert.NotNil(t, df2)
	//重复的打开同一个文件
	df3, err := OpenDataFile(DirPath, 1)
	assert.Nil(t, err)
	assert.NotNil(t, df3)

}

func TestDataFile_Write(t *testing.T) {
	//打开文件
	df1, err := OpenDataFile(DirPath, 0)
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
	df1, err := OpenDataFile(DirPath, 12)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	err = df1.Write([]byte("aa"))
	assert.Nil(t, err)
	err = df1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	df1, err := OpenDataFile(DirPath, 123)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	err = df1.Write([]byte("aa"))
	assert.Nil(t, err)
	err = df1.Sync()
	assert.Nil(t, err)
}
