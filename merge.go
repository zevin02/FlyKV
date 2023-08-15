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

	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	//将当前活跃文件转化成为旧的数据文件
	db.olderFile[db.activeFile.FileId] = db.activeFile
	//打开一个新的活跃文件，用户后续的写入都是写在当前活跃文件中，也不影响我们的merge过程
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	//记录最近没有参与merge的文件id,这个是当前用户使用的活跃文件id
	nonMergeFileId := db.activeFile.FileId

	//取出db中所有需要merge的文件
	var mergeFile []*data.DataFile
	for _, file := range db.olderFile {
		mergeFile = append(mergeFile, file)
	}

	//取出所有的需要merge的文件之后，就不需要db的锁了，后面就没有使用db的资源了
	db.mu.Unlock()

	//待merge的文件从小到大进行排序，依次merge,id越小，说明这个数据文件越旧，数据就越无效
	sort.Slice(mergeFile, func(i, j int) bool {
		return mergeFile[i].FileId < mergeFile[j].FileId
	})
	mergePath := db.getMergePath()
	//如果之前存在该目录，就需要将之前的删除掉
	if _, err := os.Stat(mergePath); err == nil {
		//该目录已经存在，删除该目录
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
	mergeDB, err := Open(mergeOption) //新打开一个db来进行处理
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
				//这里重新开一个db进行写入，他的fileId是从0开始的，并且追加写到merge的数据文件后面
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

	//对hint文件，已经merge生成的文件进行持久化，保证数据都写入到磁盘中了
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//写表示merge完成的文件,该文件中记录merge中没有包含的id值
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return nil
	}
	//value中记录当前没有参与merge的文件id,后面方便读取
	//因为merge使用的阈值和db是一样的，同时merge中写入的都是有效数据，所以文件的id一定比这个nonMergeFileId小

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
//在当前目录的同级目录中/tmp/bitcask-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath)) //当前目录的上级目录
	base := path.Base(db.options.DirPath)           //当前目录的名字
	return filepath.Join(dir, base+dirMergeName)    //生成一个新的目录路径
}

//loadMergeFiles 将merge目录中的所有文件（数据文件，hint文件，fin文件）都移动到主目录中
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		//merge目录不存在的话，就直接进行返回
		return nil
	}
	defer func() {
		//删除该目录,因为在移动完该目录中的所有文件后，该目录就没有用了
		os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//查找标识merge完成的文件，判断merge是否已经完成了
	var mergeFinished bool
	var mergeFileNames []string
	//获得merge目录下的所有文件，并且判断是否merge完成了
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			//标识merge结束的文件存在，就标识merge完成了
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name()) //将merge中用到的文件名保存起来,供后续转移
	}
	//没有merge完成，直接返回
	if !mergeFinished {
		return nil
	}
	//merge发生并完成了,从fin文件中获得最近没有参与merge的id
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//在主目录中删除比这个id小的数据文件,我们把merge目录中的文件移动过去即可替代这些数据了(都已经进行合并了)
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			//该文件存在,就需要进行删除
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	//该merge目录下的所有文件移动到正常的目录中
	for _, fileName := range mergeFileNames {
		//从bitcaskdb/mergedir    00.data     01.data
		//移动到bitcaskdb/storefile  00.data   01.data
		srcPath := filepath.Join(mergePath, fileName)           //原路径
		destPath := filepath.Join(db.options.DirPath, fileName) //新路径
		//如果原目录已经存在fin文件的话，就对他进行重写覆盖，保存只有一个fin文件
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

//获得未merge的文件id，比这个id小的文件都已经被merge了，就可以被删除掉
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	//该文件中只有一条数据
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	//record中的value中就记录了这个id
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil

}

//从hint文件中加载索引,hint中保存了key对应的位置信息
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName) //前面已经将merge目录中的文件都移动到db目录中了，所以正常使用
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		//当前的hint文件不存在，直接返回,不需要从hint文件中来构建索引
		return nil
	}
	//打开hint索引文件,根据hint文件中的记录来构建内存索引
	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}
	//hint中都是有效数据,读取数据文件
	var offset uint64 = 0
	for {
		record, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//解码获得位置信息
		pos := data.DecodeLogRecordPos(record.Value)
		//根据位置信息来构建索引
		db.index.Put(record.Key, pos)
		offset += size
	}
	return nil
}