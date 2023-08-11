package fio

import "os"

//标准系统文件IO
type FileIO struct {
	fd *os.File
}

//初始化标准文件IO对象
//fileName 要打开文件的路径名字
func NewFileIOManager(fileName string) (*FileIO, error) {
	//打开一个文件
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	//有问题旧需要返回给上层
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

//Read 从文件的给定位置读取对应的数据,
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)

}

//Write 将字节组写入到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

//Sync 将临时存在内存的数据持久化到磁盘中
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

//Close关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
