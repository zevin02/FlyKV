package main

import (
	"FlexDB"
	"FlexDB/redis/type"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
	"sync"
)

func newWrongNumberofArry(cmd string) error {
	return fmt.Errorf("ERR wrong number of argument for '%s' command", cmd)
}

type cmdHandler func(cli *FlexClient, args [][]byte) (interface{}, error)

//支持的命令
var supportedCommands = map[string]cmdHandler{
	"set":    set,
	"get":    get,
	"select": Select,
}

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
		switch command {
		case "quit":
			conn.Close()
		case "ping":
			conn.WriteString("PONG")
		default:
			conn.WriteError("Err unsupport command: '" + command + "'")
		}
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
