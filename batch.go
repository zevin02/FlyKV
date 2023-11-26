package FlexDB

import (
	"FlexDB/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeq uint64 = 0

var txnFinKey = []byte("txn-fin")

//WriteBatch 原子批量写数据，保证原子性
type WriteBatch struct {
	options      WriteBatchOptions
	mu           *sync.Mutex
	db           *DB
	pendingWrite map[string]*data.LogRecord //暂存用户写入的数据
}

//NewWriteBatch 初始化WriteBatch
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	//如果是B+树，同时事务序列号文件不存在（不存在可能是第一次进来），且不是第一次加载数据库的时候，就要panic
	//B+树禁止writebatch可以提高写入性能，B+树中使用会增加写入的锁竞争（避免长时间占用锁）和内存消耗（不需要内存额外维护一个缓冲区），
	if db.options.IndexType == BPT && !db.seqNoFileExists && !db.isInitialDBInitialized {
		panic("can not use write batch,seqno file not exist")
	}
	return &WriteBatch{
		options:      options,
		mu:           new(sync.Mutex),
		db:           db,
		pendingWrite: make(map[string]*data.LogRecord),
	}
}

//Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	//将LogRecord 暂存起来
	logRecord := &data.LogRecord{Key: key, Value: value, Type: data.LogRecordNormal}
	wb.pendingWrite[string(key)] = logRecord
	return nil
}

//Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	node, err := wb.db.hashRing.Get(string(key)) //获得对应实例
	if err != nil {
		return err
	}
	//先在内存索引中查看数据是否存在
	logRecordPos := wb.db.index[node].Get(key)
	if logRecordPos == nil {
		//数据不存在
		if wb.pendingWrite[string(key)] != nil {
			//数据暂存在批处理中,就需要将该数据从中进行删除
			delete(wb.pendingWrite, string(key))
		}
		return nil
	}

	//该数据存在

	//将LogRecord 暂存起来
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrite[string(key)] = logRecord
	return nil
}

//Commit 将批量数据全部写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	//加锁保证事务提交的串形化
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if len(wb.pendingWrite) == 0 {
		//当前事务中没有数据,直接返回
		return nil
	}
	if uint(len(wb.pendingWrite)) > wb.options.MaxWriteNum {
		//超过了一个批处理的上限值，就出错了
		return ErrExceedMaxBatchNum
	}

	//获取当前最新事务的序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1) //原子加1
	//内存索引信息保存
	postions := make(map[string]*data.LogRecordPos)
	//开始写数据到数据文件中
	for _, record := range wb.pendingWrite {
		//记录批量写入，具有相同的事务序列号，同时上面已经加锁了，这里就不需要再加锁
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return nil
		}
		postions[string(record.Key)] = logRecordPos

	}

	//写入一条标注事务结束的数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}
	//根据配置进行持久化
	if wb.options.SyncWrite {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	//根据前面append获得的postion映射，来更新内存索引
	for _, record := range wb.pendingWrite {
		pos := postions[string(record.Key)]                 //获得该数据的位置信息
		node, err := wb.db.hashRing.Get(string(record.Key)) //获得对应实例
		if err != nil {
			return err
		}
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			//正常数据，就正常进行更新
			oldPos = wb.db.index[node].Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			//数据需要从内存中进行一个删除
			oldPos, _ = wb.db.index[node].Delete(record.Key)

		}
		if oldPos != nil {
			wb.db.reclaimSize += uint64(oldPos.Size)
		}

	}
	//清空暂存数据
	wb.pendingWrite = make(map[string]*data.LogRecord)
	return nil
}

//SeqNo+key 进行编码,编码出一个新的key
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	//将序列号先进行编码
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, len(key)+n)

	//先拷贝进去序列号
	copy(encKey[:n], seq[:n])
	//再拷贝进去具体的key值
	copy(encKey[n:], key)
	return encKey
}

//解析LogRecord的key，获取实际的key和事务
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
