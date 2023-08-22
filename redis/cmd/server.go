package main

import (
	"FlexDB"
	"FlexDB/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type FlexServer struct {
	dbs    map[int]*redis.RedisDataStruct //用户的数据库
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	//打开redis数据结构的服务
	rds, err := redis.NewRedisDataStruct(FlexDB.DefaultOperations)
	if err != nil {
		panic(err)
	}
	dbSvr := FlexServer{
		dbs: make(map[int]*redis.RedisDataStruct),
	}
	dbSvr.dbs[0] = rds

	//初始化redis服务器,设置建立连接的回调函数,断开连接的回调函数，以及客户端进来执行的回调函数
	dbSvr.server = redcon.NewServer(addr, execClientCommand, dbSvr.Accept, dbSvr.Close)
	dbSvr.Listen()

}

func (svr *FlexServer) Listen() {
	log.Println("FlexDB server running,ready to accept connection")
	svr.server.ListenAndServe()
}

//传递连接进来
func (svr *FlexServer) Accept(conn redcon.Conn) bool {
	cli := new(FlexClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.db = svr.dbs[0]
	cli.svr = svr
	conn.SetContext(cli) //执行的时候，从这个中取出来
	return true
}

//关闭实例
func (svr *FlexServer) Close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		db.Close()
	}
	svr.server.Close()
}
