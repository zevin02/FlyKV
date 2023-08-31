package FlexDB

import (
	"log"
	"time"
)

//startBackgroundTask 执行一些后台需要执行的代码
func (db *DB) startBackgroundTask() {
	//创建一些定时触发的操作

	flushTicker := time.NewTicker(time.Duration(db.options.TimeSync) * time.Second) //创建刷盘定时器
	defer flushTicker.Stop()
	for {
		select {
		case <-flushTicker.C:
			//刷盘的时间到了,在这个地方进行刷盘
			if err := db.Sync(); err != nil {
				log.Printf("Flush error :%s \n", err)
			}
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
			//判断是否需要进行merge操作

		}

	}
}
