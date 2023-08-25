package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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

// CopyDir 拷贝数据目录
func CopyDir(src, dst string, exclude []string) error {
	//目标不存在则创建
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err := os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}
	//递归遍历整个数据目录
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1) //取出路径中的文件名
		if fileName == "" {
			return nil
		}
		//查看该文件是否包含在排除的文件集合中
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() {
			//如果是文件夹的话，就需要在目标路径中创建一个相同的文件夹
			return os.MkdirAll(filepath.Join(dst, fileName), info.Mode())
		}
		//普通文件的话就需要读取数据并写入到指定位置
		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, fileName), data, info.Mode())
	})

}
