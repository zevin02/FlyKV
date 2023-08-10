package BitcaskDB

import (
	"BitcaskDB/data"
	"BitcaskDB/index"
	"sync"
)

//DB Bitcask存储引擎的实例
type DB struct {
	options    Options //配置信息
	mu         *sync.RWMutex
	activeFile *data.DataFile            //当前活跃文件，可以用来写入
	olderFile  map[uint32]*data.DataFile //旧的数据文件，用来读取
	index      index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//构造LogRecord结构体
	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}
	//获得索引信息，更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

}

//根据Key读取数据
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
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//如果获得的数据已经删除，就不允许返回，说明没有找到
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil

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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
