package BitcaskDB

import (
	"BitcaskDB/data"
	"BitcaskDB/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

//DB Bitcask存储引擎的实例
type DB struct {
	fileIds    []int   //文件ID，只能在加载索引的时候使用
	options    Options //配置信息
	mu         *sync.RWMutex
	activeFile *data.DataFile            //当前活跃文件，可以用来写入
	olderFile  map[uint32]*data.DataFile //旧的数据文件，用来读取
	index      index.Indexer             //数据的内存索引
}

//Open 打开bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//判断目录是否存在，不存在就需要进行创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		//创建目录
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化DB的实例，并对数据结构进行初始化
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndex(options.IndexType),
	}
	//加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}
	//从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

//Put 将key和value添加到数据库中
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	//获得索引信息，更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

}

//Get 根据Key读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	//打开读锁
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	//从内存中拿出索引位置信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
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

//根据key删除对应的数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//在内存索引中查找这个key是否存在,避免用户一致调用delete方法去删除一个不存在的key，导致磁盘文件膨胀
	if pos := db.index.Get(key); pos == nil {
		//当前key不存在，直接返回
		return nil
	}
	//构造LogRecord标识其是被删除的
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	//写入到数据文件中
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	//在内存索引中将对应的key删除掉
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

//插入后会返回这个位置的索引信息
//追加数据写入到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

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
		//超过了阈值
		//先进行持久化,保证数据都已经持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//设置进旧的文件集合中
		db.olderFile[db.activeFile.FileId] = db.activeFile
		//设置新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff
	//写入到文件中
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//判断是否需要对数据进行安全的持久化操作
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	//返回位置信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil

}

//设置当前活跃文件
//在访问这个方法的时候必须要持有锁，并发可能会有很多操作
func (db *DB) setActiveDataFile() error {
	//设置初始的activeID
	var initialFileId uint32 = 0
	//如果active已经存在了，就说明前面的active文件已经写到阈值了，需要新开一个文件了
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId) //打开一个新的活跃文件用于读写
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
	return nil
}

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

			splitNames := strings.Split(entry.Name(), ".")
			//获得文件ID
			fileId, err := strconv.Atoi(splitNames[0])

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
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
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

//从数据文件中读取数据构造索引
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，说明当前是一个空的数据库
	if len(db.fileIds) == 0 {
		return nil
	}
	//遍历所有的文件ID，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
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
			var ok bool
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			if logRecord.Type == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			//递增offset，下一次从新的位置开始读取
			offset += size
		}
		//如果当前为活跃文件。就需要更新这个文件的WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
}
