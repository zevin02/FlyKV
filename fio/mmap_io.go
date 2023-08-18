package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

//MMap IO,内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(filename string) (*MMap, error) {
	//如果文件不存在就要创建
	_, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename) //打开特定的需要mmap的文件
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil

}

//Read 从文件的给定位置读取对应的数据到b，返回读取的字符数
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

//Write 我们的mmap不实现write功能
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

//Sync 将临时存在内存的数据持久化到磁盘中
func (mmap *MMap) Sync() error {
	panic("not implemented")

}

//Close关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()

}

//获得IO对象大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
