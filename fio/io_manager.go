package fio

const DataFilePerm = 0644

//我们封装IOmanager方便后续添加不同的IO类型

type IOManager interface {
	//Read 从文件的给定位置读取对应的数据到b，返回读取的字符数
	Read([]byte, int64) (int, error)
	//Write 将字节组写入到文件中
	Write([]byte) (int, error)
	//Sync 将临时存在内存的数据持久化到磁盘中
	Sync() error

	//Close关闭文件
	Close() error

	//获得IO对象大小
	Size() (int64, error)
}

//初始化IOManger,目前只有一个FileIO
func NewIOManager(filename string) (IOManager, error) {
	return NewFileIOManager(filename)
}
