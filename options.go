package BitcaskDB

type Options struct {
	DirPath     string    //数据库数据目录
	FileSize    uint64    //活跃文件的阈值
	SyncWrite   bool      //是否在每次写都进行持久化
	IndexType   IndexType //索引类型
	BytePerSync uint64    //累积写了多少字节后进行持久化
	//TODO 添加后台线程来处理
	TimeSync           uint    //每隔多少秒就进行一次持久化
	MMapAtStartup      bool    //在启动的时候使用使用mmap来加载
	DataFileMergeRatio float32 //数据文件的无效数据达到多少的数据文件多少比例进行merge的阈值
}

type IndexType = int8

const (
	//Btree 索引
	Btree IndexType = iota
	//ART 自适应基数树索引
	ART
	//B+树索引
	BPT
)

var DefaultOperations = Options{
	DirPath:            string("/home/zevin/githubmanage/program/BitcaskDB/storefile"),
	FileSize:           256 * 1024 * 1024, //256MB
	SyncWrite:          false,
	IndexType:          Btree,
	BytePerSync:        0,
	TimeSync:           0,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

//IteratorOptions 索引迭代器的配置项
type IteratorOptions struct {
	//遍历前缀为指定的Key，默认为空
	Prefix []byte
	//是否为反向遍历
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	//一个批次中最多能写入多少的数据
	MaxWriteNum uint
	//在写入到磁盘的时候是否需要进行一个持久化
	SyncWrite bool
}

var DefaultWriteBatchOption = WriteBatchOptions{
	MaxWriteNum: 10000,
	SyncWrite:   true,
}
