package cmd

import (
	"FlexDB"
	"FlexDB/redis/type"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type FlexServer struct {
	db     *_type.RedisDataStruct //用户的数据库，允许开16个
	server *redcon.Server
	mu     *sync.RWMutex
}

func NewServer() (*FlexServer, error) {
	//默认是打开redis数据结构的服务
	rds, err := _type.NewRedisDataStruct(FlexDB.DefaultOperations)
	if err != nil {
		panic(err)
	}
	dbSvr := &FlexServer{
		db: rds,
		mu: new(sync.RWMutex),
	}
	dbSvr.server = redcon.NewServer(addr, execClientCommand, dbSvr.Accept, dbSvr.Close)

	return dbSvr, nil
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
	cli.db = svr.db
	cli.svr = svr
	cli.mu = new(sync.RWMutex)
	conn.SetContext(cli) //执行的时候，从这个中取出来
	return true
}

//关闭实例
func (svr *FlexServer) Close(conn redcon.Conn, err error) {
	svr.db.Close()
	svr.server.Close()
}
