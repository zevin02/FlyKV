package BitcaskDB

type Options struct {
	DirPath   string //数据库数据目录
	FileSize  uint64 //活跃文件的阈值
	SyncWrite bool
}
