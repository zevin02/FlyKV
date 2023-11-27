package FlexDB

import (
	"FlexDB/data"
	"FlexDB/fio"
	"FlexDB/index"
	"FlexDB/mvcc"
	"FlexDB/utils"
	"encoding/binary"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"stathat.com/c/consistent"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey      = "seqNoKey"
	fileFlockName = "fileFlcok"
)

//使用key找到他的keyindex
//DB Bitcask存储引擎的实例
type DB struct {
	fileIds                []int   //文件ID，只能在加载索引的时候使用
	options                Options //配置信息
	mu                     *sync.RWMutex
	activeFile             *data.DataFile            //当前活跃文件，可以用来写入,在加载数据文件的时候，活跃文件和老文件都会被初始化
	olderFile              map[uint32]*data.DataFile //旧的数据文件，用来读取
	hashRing               *consistent.Consistent    //一致性哈希环，用来保证数据负载均衡式的分配到各个索引中
	index                  map[string]index.Indexer  //数据的内存索引,TODO 索引数据可以定期写入到磁盘中，保证故障回复,B+树本身就是写入到磁盘的，
	seqNo                  uint64                    //事务序列号，全局递增
	seqNoFileExists        bool                      //存储事务序列号的文件是否存在
	isInitialDBInitialized bool                      //是否是第一次初始化此数据目录
	fileLock               *flock.Flock              //当前进程持有的文件锁,保证多进程之间互斥
	ByteWritten            uint64                    //记录一个周期中写入的字节数
	reclaimSize            uint64                    //这个是记录当前有多少字节是无效的
	mergeInfo              MergeInfo                 //保存merge相关信息
	exitSignal             chan struct{}             //退出信号的管道，用于控制Goroutine的退出
	stat                   *Stat                     //记录某一个时刻的db的状态
	latestRevison          int64                     //下一次进来需要使用的版本号,修改，之后使用seqNo
	versionIndex           *mvcc.TreeIndex           //全局只能拥有一个TreeIndex，这个是内存级别的，所以在db启动的时候，就需要构造这个对象,我们可以使用WAL，把数据存储在WAL中
}

//Stat 可以记录某一个时刻的db状态
type Stat struct {
	KeyNum          int    //key的总数量
	DataFileNum     uint   //磁盘中数据文件的数量
	ReclaimableSize uint64 //可以进行merge回收的数据量
	DiskSize        uint64 //所占磁盘空间的大小
}

//Open 打开bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//判断目录是否存在，不存在就需要进行创建目录,存在的话，就没有操作
	var isInitial = false //判断是否第一次初始化数据库目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		//创建目录
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//判断当前的数据目录是否正在被使用，一个进程实例只能对应一个目录
	fileFlock := flock.New(filepath.Join(options.DirPath, fileFlockName))
	hold, err := fileFlock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		//没有获得锁，说明这个锁被其他进程给使用了
		return nil, ErrDataBaseIsUsing
	}
	//初始化DB的实例，并对数据结构进行初始化
	db := &DB{
		options:                options,
		mu:                     new(sync.RWMutex),
		olderFile:              make(map[uint32]*data.DataFile),
		hashRing:               consistent.New(),                                 //
		index:                  make(map[string]index.Indexer, options.indexNum), //初始化内存索引
		seqNo:                  nonTransactionSeq,
		isInitialDBInitialized: isInitial,
		fileLock:               fileFlock,
		exitSignal:             make(chan struct{}),
		versionIndex:           mvcc.NewTreeIndex(), //初始化一个版本的索引树，当前的数据还没有实现对数据的持久化
	}
	db.initIndex()
	//加载merge数据目录,将merge目录下的数据都移动过来
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	defer func() {
		//完成merge后需要把关于merge的信息清空
		db.mergeInfo = MergeInfo{}
	}()

	//正常的在进行加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	//加载内存索引
	//非b+树是把索引存储在内存中
	if options.IndexType != BPT {
		if err := db.loadIndex(); err != nil {
			return nil, err
		}
	}

	//b+树是把索引存储在磁盘中,所以不需要把数据读取到内存中，需要的时候读取即可,取出当前的事务号
	if options.IndexType == BPT {
		//加载事务序列号(merge的时候)
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		//B+树的active文件需要更新
		//对于B+树模型，不会更新offset，所以这里要手动的更新active文件的offset
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = uint64(size)
		}
	}
	if db.options.MMapAtStartup {
		//如果使用MMap加速启动的话，active文件是只读不能写的，所以我们需要设置成标准文件类型
		if err := db.setIoManger(fio.StanderFIO); err != nil {
			return nil, err
		}
	}

	//启动goroutine处理定时任务
	go db.startBackgroundTask()
	return db, nil
}

//Put 将key和value添加到数据库中
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	rev := mvcc.Revision{Main: db.latestRevison, Sub: 0}
	//更新当前的版本号
	//TODO db.latestRevision use atomic addition seqNo := atomic.AddUint64(&wb.db.seqNo, 1) //原子加1

	db.latestRevison++
	revEncoded := rev.Encode()
	//将当前的版本版本链信息添加到keyIndex中进行管理
	db.VersionPut(key, rev)
	key = append(key, revEncoded...) //当前的key追加上这个序列化之后的版本号信息

	//构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeq), //普通的key也加上这个，来辨别是否为事务
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	//获得索引信息，更新内存索引,内存索引中的key就是用户的key，没有进行任何的编码
	node, err := db.hashRing.Get(string(key)) //获得对应实例
	if err != nil {
		return err
	}
	if oldPos := db.index[node].Put(key, pos); oldPos != nil {
		//如果有数据，则出现无效数据，存在磁盘里，但内存中已更新。
		db.reclaimSize += uint64(oldPos.Size)
	}
	return nil
}

//Get 根据Key读取数据,根据当前的revision信息进行处理
//TODO 可以实现一个读缓存来存储一些数据，避免每次直接进行磁盘IO，可以考虑使用LRU（用到节点中里面的timestamp和内存索引的timestamp比较，看是否返回），同时也可以考虑使用布隆过滤器来过滤没找到的key，就不需要要取查找
func (db *DB) Get(key []byte) ([]byte, error) {
	//打开读锁
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//在这里使用revisionIndex，在版本链中查找到指定的revision信息
	rev, err := db.VersionGet(key)
	if rev == nil || err != nil {
		//当前的versionIndex中
		return nil, ErrKeyNotFound
	}
	key = append(key, rev.Encode()...)
	//更新当前的版本号
	db.latestRevison++

	//从内存中拿出索引位置信息
	node, err := db.hashRing.Get(string(key)) //获得对应实例
	if err != nil {
		return nil, err
	}
	logRecordPos := db.index[node].Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	//从数据文件中获取value
	return db.getValueByPos(logRecordPos)

}

//getValueByPos 根据位置信息获取value
func (db *DB) getValueByPos(logRecordPos *data.LogRecordPos) ([]byte, error) {

	//获得到位置信息
	//根据文件Id找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFile[logRecordPos.Fid]
	}
	//数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//找到了对应的数据文件，根据其偏移量来读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//如果获得的数据已经删除，就不允许返回，说明没有找到
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

//ListKeys 获取数据中所有的key
func (db *DB) ListKeys(options IteratorOptions) [][]byte {
	var keys [][]byte
	iterator := db.NewIterator(options)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys = append(keys, iterator.Key())
	}
	return keys
}

//Fold 获取所有的数据，并执行用户指定的操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool, options IteratorOptions) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.NewIterator(options)
	//使用完需要将他关闭掉,避免读写阻塞住
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		val := iterator.Value()
		//如果不满足用户需求就跳出循环
		if !fn(iterator.Key(), val) {
			break
		}
	}
	return nil

}

//Delete 根据key删除对应的数据,如果存在的话，返回true，否则返回失败
func (db *DB) Delete(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	rev := mvcc.Revision{Main: db.latestRevison, Sub: 0}
	db.VersionDelete(key, rev)
	db.latestRevison++
	revEncoded := rev.Encode()
	key = append(key, revEncoded...) //当前的key追加上这个序列化之后的版本号信息

	//在内存索引中查找这个key是否存在,避免用户一致调用delete方法去删除一个不存在的key，导致磁盘文件膨胀
	node, err := db.hashRing.Get(string(key)) //获得对应实例
	if err != nil {
		return false, err
	}
	if pos := db.index[node].Get(key); pos == nil {
		//当前key不存在，直接返回
		return false, nil
	}

	//构造LogRecord标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeq),
		Type: data.LogRecordDeleted,
	}
	//写入到数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return false, err
	}
	//删除的这个数据本身也是无效数据存储在磁盘中,也是可以删除的
	db.reclaimSize += uint64(pos.Size)

	if err != nil {
		return false, err
	}
	//在内存索引中将对应的key删除掉
	oldPos, ok := db.index[node].Delete(key)

	if !ok {
		return false, ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += uint64(oldPos.Size)
	}
	return true, nil
}

// Close 关闭数据库,清空所有的资源
func (db *DB) Close() error {

	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic("fail to unlock the directory")
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	close(db.exitSignal) //发送退出信号给goRuntine
	// 等待后台 Goroutine 完全退出
	<-db.exitSignal
	//关闭索引
	for i := 0; i < db.options.indexNum; i++ {
		node := "index" + strconv.Itoa(i)
		if err := db.index[node].Close(); err != nil {
			return err
		}
	}
	//保存当前的事务序列号，B+树需要
	if err := db.saveSeqNo(); err != nil {
		return err
	}
	//关闭活跃文件和老文件
	if err := db.closeFiles(); err != nil {
		return err
	}

	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

//Stat 获得当前db的状态,可以放到后台线程来进行执行,不定时进行更新
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var dataFiles = uint(len(db.olderFile))
	if db.activeFile != nil {
		dataFiles += 1
	}
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		return nil
	}
	return &Stat{
		//KeyNum:          db.index.Size(),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        totalSize,
	}

}

//BackUp 数据备份，直接将数据目录进行拷贝，就可以实现做备份了
func (db *DB) BackUp(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	//拷贝文件的时候文件锁不能拷贝过去，一个目录只能包含一个文件锁
	return utils.CopyDir(db.options.DirPath, dir, []string{fileFlockName})
}

//appendLogRecordWithLock 加锁的写入
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

//appendLogRecord 插入后会返回这个位置的索引信息
//追加数据写入到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	//判断当前活跃活跃文件是否存在
	//如果为空，则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	//持有了当前活跃文件
	encRecord, size := data.EncodeLogRecord(logRecord)
	//如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件（标记为旧文件），并打开新的活跃文件
	if db.activeFile.WriteOff+size > db.options.FileSize {
		//由于当前的活跃文件的大小超过了阈值，所以需要将该活跃文件先进行持久化到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//持久化之后，修改成MMap方式,无法进行修改，加快文件的读取
		err := db.setIoManger(fio.MMapFio)
		if err != nil {
			return nil, err
		}
		//设置进旧的文件集合中
		db.olderFile[db.activeFile.FileId] = db.activeFile
		//再打开一个新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff //返回数据在文件中的偏移位置
	//写入到文件中
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.ByteWritten += size
	//binary.LittleEndian.Uint32(encRecord[5:9])

	////判断是否需要对数据进行安全的持久化操作
	//var needSync bool = db.options.SyncWrite
	////写入的字节数到达用户要求的perSync的倍数就要进行持久化操作
	//if !needSync && db.options.BytePerSync > 0 && db.ByteWritten > db.options.BytePerSync {
	//	needSync = true
	//}
	if db.needSync() {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.ByteWritten = 0 //重新将数据进行清零

	}

	//返回位置信息,包含当前的位置信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size), Tstamp: binary.LittleEndian.Uint32(encRecord[5:9])}

	return pos, nil

}

//needSync 判断当前是否需要进行持久化
func (db *DB) needSync() bool {
	var needSync = db.options.SyncWrite
	//写入的字节数到达用户要求的perSync的倍数就要进行持久化操作
	if !needSync && db.options.BytePerSync > 0 && db.ByteWritten > db.options.BytePerSync {
		needSync = true
	}
	return needSync
}

//setActiveDataFile 设置当前活跃文件
//在访问这个方法的时候必须要持有锁，并发可能会有很多操作
func (db *DB) setActiveDataFile() error {
	//设置初始的activeID
	var initialFileId uint32 = 0
	//如果active已经存在了，就说明前面的active文件已经写到阈值了，需要新开一个文件了
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	//这个地方打开的文件需要使用标准IO的
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StanderFIO) //打开一个新的活跃文件用于读写
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

//对配置项进行校验
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDirIsInValid
	}
	if options.FileSize <= 0 {
		return ErrFileSizeInValid
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return ErrMergeRatio
	}
	return nil
}

//loadDataFile 加载数据文件
func (db *DB) loadDataFile() error {
	//读目录读取出来，把该目录中的所有文件读取出来
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//遍历目录中的所有文件,找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			//对00001.data文件进行分割，拿到他的第一个部分00001

			trimmedName := strings.TrimLeft(entry.Name()[:len(entry.Name())-len(".data")], "0") //去掉前导0
			// 转换为文件ID
			if trimmedName == "" {
				trimmedName = "0"
			}
			//获得文件ID
			fileId, err := strconv.Atoi(trimmedName) //获得文件ID
			if err != nil {
				return ErrDataDirCorrupted
			}
			fileIds = append(fileIds, fileId)

		}
	}
	//对文件ID进行排序，从小到大
	sort.Ints(fileIds)
	db.fileIds = fileIds //获得目录下的所有文件名，供后续读取文件构建索引

	//遍历每个文件ID，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StanderFIO
		if db.options.MMapAtStartup {
			//在启动的使用Mmap加速读取文件来构建索引
			ioType = fio.MMapFio
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			//说明这个是最后一个id，就设置成活跃文件
			db.activeFile = dataFile
		} else {
			//否则就放入到旧文件集合中
			db.olderFile[uint32(fid)] = dataFile
		}

	}
	return nil
}

//loadIndexFromDataFiles 从数据文件中读取数据构造索引
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，说明当前是一个空的数据库
	if len(db.fileIds) == 0 {
		return nil
	}

	//更新内存索引,
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		node, _ := db.hashRing.Get(string(key)) //获得对应实例

		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index[node].Delete(key)
			db.reclaimSize += uint64(pos.Size)

		} else {
			oldPos = db.index[node].Put(key, pos)
		}
		//如果构建索引的时候，这个key之前已经被存在了，那么这个key之前的数据就是无效的，可以进行清理
		if oldPos != nil {
			db.reclaimSize += uint64(oldPos.Size)
		}
	}

	//暂存事务的数据,一个事务里面是有多个数据的
	transactionRecord := make(map[uint64][]*data.TransactionRecord)
	var curSeqNo = nonTransactionSeq
	//遍历所有的文件ID，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		//如果比最近未参与merge的文件id小，说明已经从hint文件中加载了索引
		if db.mergeInfo.hashMerged && fileId < db.mergeInfo.nonMergeFildId {
			continue
		}
		//merge完的数据都被消除了事务的标志，merge之后写入的数据仍然保持有事务的id
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			//当前文件是活跃文件，就从活跃文件中获得
			dataFile = db.activeFile
		} else {
			//当前文件是旧文件，就从旧文件中根据ID号码获得
			dataFile = db.olderFile[fileId]
		}
		var offset uint64 = 0
		//读取当前文件的数据，根据读取的数据来构造索引
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset) //根据offset读取一条log记录
			if err != nil {
				//文件读取完了
				if err == io.EOF {
					break
				} else {
					return err
				}
			}
			//构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			//解析key，拿到事务的ID
			key, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeq {
				//非事务提交,直接更新索引
				updateIndex(key, logRecord.Type, logRecordPos)
			} else {
				//是事务提交
				if logRecord.Type == data.LogRecordTxnFinished {
					//事务完成，将对应的seq no的数据一次性进行更新,如果没有这个标志的话，内存索引就不会更新，实现了原子性质
					for _, txnRecord := range transactionRecord[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecord, seqNo)
				} else {
					//还没有达到事务的结束,先将读取到的数据暂存起来
					logRecord.Key = key //在内存中我们使用的是没有编码的key
					transactionRecord[seqNo] = append(transactionRecord[seqNo], &data.TransactionRecord{
						Pos:    logRecordPos,
						Record: logRecord,
					})

				}

			}
			//更新当前的事务序列号
			if seqNo > curSeqNo {
				curSeqNo = seqNo
			}

			//递增offset，下一次从新的位置开始读取
			offset += size
		}
		//如果当前为活跃文件(读到最后一个文件没有写满，我们需要拿到他的偏移量，继续写，直到把他写满)。就需要更新这个文件的WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
		//更新事务序列号
		db.seqNo = curSeqNo
	}
	return nil
}

//loadSeqNo 加载事务序列号文件,获得事务序列号
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	//加载完我们就需要将这个文件删除，避免追加写，我们对于这个文件只需要一条数据即可
	return os.Remove(fileName)
}

//setIoManger 设置文件IO的方法
func (db *DB) setIoManger(managerType fio.IOManagerType) error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.SetIOManager(db.options.DirPath, managerType); err != nil {
		return err
	}

	return nil
}

func (db *DB) loadIndex() error {
	//从hint文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}

	//从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return err
	}
	return nil

}

func (db *DB) closeFiles() error {
	//关闭当前的所有文件
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	db.activeFile = nil

	for _, file := range db.olderFile {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) saveSeqNo() error {
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)

	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}
	if err := seqNoFile.Close(); err != nil {
		return nil
	}
	return nil
}

//初始化索引
func (db *DB) initIndex() {
	// 添加5个索引节点
	for i := 0; i < db.options.indexNum; i++ {
		node := "index" + strconv.Itoa(i)
		db.hashRing.Add(node)

		db.index[node] = index.NewIndex(db.options.IndexType, db.options.DirPath, node, db.options.SyncWrite) //初始化内存索引
	}

}

//VersionPut 在版本索引的key版本链中添加一个版本
func (db *DB) VersionPut(key []byte, rev mvcc.Revision) {
	db.versionIndex.Put(key, rev)
}

//VersionGet 根据key获得对应的版本链信息
func (db *DB) VersionGet(key []byte) (*mvcc.Revision, error) {
	return db.versionIndex.Get(key, db.latestRevison)
}

//VersionDelete 在当前的版本链表中删除一个版本
func (db *DB) VersionDelete(key []byte, revision mvcc.Revision) {
	db.versionIndex.Tombstone(key, revision)
}
