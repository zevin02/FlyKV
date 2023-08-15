package BitcaskDB

import (
	"BitcaskDB/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	dirMergeName     = "-merge"
	mergeFinishedKey = "mergeFinished-KEY"
)

//Merge 清理无效数据，生成hint文件
func (db *DB) Merge() error {
	//如果数据库为空，直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	if db.isMerging {
		//同一时刻只能存在一个merge过程，当前已经处在merge阶段了,直接返回
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	//设置merge过程的标识
	db.isMerging = true
	defer func() {
		//该过程退出的时候，进行资源清理，结束merge标识
		db.isMerging = false
	}()

	//0 1 2,我们打开一个新的俄
	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	//将当前活跃文件转化成为旧的数据文件
	db.olderFile[db.activeFile.FileId] = db.activeFile
	//打开一个新的活跃文件，用户后续的写入都是写在当前活跃文件中
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	//记录最近没有参与merge的文件id
	nonMergeFileId := db.activeFile.FileId

	//取出db中所有需要merge的文件
	var mergeFile []*data.DataFile
	for _, file := range db.olderFile {
		mergeFile = append(mergeFile, file)
	}

	//取出所有的需要merge的文件之后，旧不需要db的锁了
	db.mu.Unlock()
	//待merge的文件从小到大进行排序，依次merge,id越小，说明这个数据文件越旧，数据就越无效
	sort.Slice(mergeFile, func(i, j int) bool {
		return mergeFile[i].FileId < mergeFile[j].FileId
	})
	mergePath := db.getMergePath()
	//如果之前存在该目录，就需要将之前的删除掉
	if _, err := os.Stat(mergePath); err == nil {
		//删除该目录
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//新建一个merge path目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	//打开一个新的临时的bitcask实例
	mergeOption := db.options
	mergeOption.DirPath = mergePath
	//不需要每次都进行sync，可以在写完进行统一的统一的sync，避免太慢
	mergeOption.SyncWrite = false
	mergeDB, err := Open(mergeOption)
	if err != nil {
		return err
	}
	//打开一个hint文件，保存位置索引信息
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//遍历处理每个数据文件
	for _, dataFile := range mergeFile {
		var offset uint64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//文件读取完了
				if err == io.EOF {
					break
				} else {
					return err
				}
			}
			//解析拿到实际的key,这里我们就不需要使用到事务，因为每一条数据都是有效的了,被重写的
			realKey, _ := parseLogRecordKey(logRecord.Key)

			logRecordPos := mergeDB.index.Get(realKey)
			//和内存中的索引位置进行比较，如果有效就进行重写
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				//内存中的数据都是真实有效的，所以如果和内存中的数据相同就没有问题
				//重写，清除事务的标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeq)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//将当前的位置索引信息添加到HINT文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}

			}
			//递增offset
			offset += size

		}
	}
	//对当前文件进行持久化，保证数据都写入到磁盘中了,merge文件只有一个
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//写标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return nil
	}
	//value中记录当前没有参与merge的文件id
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

//tmp/bitcask
//在当前目录的同级目录中
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath)) //当前目录的上级目录
	base := path.Base(db.options.DirPath)           //当前目录的名字
	return filepath.Join(dir, base+dirMergeName)    //生成一个新的目录路径
}
