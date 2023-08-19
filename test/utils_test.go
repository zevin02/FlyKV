package test

import (
	"BitcaskDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(utils.GetTestKey(i)))
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(utils.RandomValue(10)))
	}

}
func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd() //获得当前文件所在目录的绝对路径
	dirSize, err := utils.DirSize(dir)
	assert.Nil(t, err)
	assert.True(t, dirSize > 0)
}

func TestAvailableDiskSize(t *testing.T) {
	size, _ := utils.AvailableDiskSize()
	//t.Log(size / 1024 / 1024 / 1024)
	assert.NotNil(t, size)
}
