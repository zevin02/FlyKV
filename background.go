package FlexDB

import (
	"log"
	"time"
)

//const revInterval = 60 * time.Second

//startBackgroundTask 执行一些后台需要执行的代码
func (db *DB) startBackgroundTask() {
	//创建一些定时触发的操作

	flushTicker := time.NewTicker(time.Duration(db.options.TimeSync) * time.Second) //创建刷盘定时器
	StatTicker := time.NewTicker(time.Duration(db.options.TimeGetStat) * time.Second)
	//CompactTicker := time.NewTicker(time.Duration(revInterval) * time.Second) //定时5分钟进行对数据进行压缩
	defer flushTicker.Stop()
	for {
		select {
		case <-flushTicker.C:
			//刷盘的时间到了,在这个地方进行刷盘
			if err := db.Sync(); err != nil {
				log.Printf("Flush error :%s \n", err)
			}
		case <-StatTicker.C:
			db.Stat()
		//case <-CompactTicker.C:
		//当前的时间到了，就要进行压缩，采样一定的数量

		case <-db.exitSignal:
			//如果用户Close DB，就退出当前的goroutine
			return
		default:
			//当打到一定的数据量就进行对数据进行持久化
			if db.needSync() {
				if err := db.Sync(); err != nil {
					log.Printf("Flush error :%s \n", err)
				}
			}
			//判断是否需要进行merge操作,如果打到阈值才开始操作
			if db.stat != nil && db.stat.DiskSize != 0 && float32(db.reclaimSize)/float32(db.stat.DiskSize) > db.options.DataFileMergeRatio {
				if err := db.Merge(false); err != nil {
					log.Printf("Background Merge error:%s \n", err)
				}
			}

		}

	}
}
