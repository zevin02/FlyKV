package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
)

//DirSize 获得一个目录的大小
func DirSize(dirPath string) (uint64, error) {
	var size int64
	//递归遍历当前目录，统计所有文件大小
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return uint64(size), err
}

// AvailableDiskSize 获得磁盘剩余可以用的空间大小
func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd() //获得当前工作目录
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t //存储当前目录的磁盘信息
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
