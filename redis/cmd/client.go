package cmd

import (
	"FlexDB"
	"FlexDB/redis/type"
	"github.com/tidwall/redcon"
	"strings"
	"sync"
)

//type cmdHandler func(cli *FlexClient, args [][]byte) (interface{}, error)

type FlexClient struct {
	db      *_type.RedisDataStruct
	svr     *FlexServer
	dbIndex byte //用户使用的是哪个数据库,后期所有key都进行编码添加上所属的dbindex实例
	mu      *sync.RWMutex
}

//command中就是用户提供的命令
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0])) //获得命令的类型
	cmdFunc, ok := supportedCommands[command]       //在列表中找到处理函数
	if !ok {
		execGeneralRedisCommand(command, conn)
		return
	}

	//拿出客户端
	client, _ := conn.Context().(*FlexClient)

	res, err := cmdFunc(client, cmd.Args[1:])
	if err != nil {
		if err == FlexDB.ErrKeyNotFound {
			conn.WriteNull()
		} else {
			conn.WriteError(err.Error())
		}
		return
	}
	conn.WriteAny(res)
}

func execGeneralRedisCommand(cmd string, conn redcon.Conn) {
	switch cmd {
	case "quit":
		conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		conn.WriteError("Err unsupport command: '" + cmd + "'")
	}
}
