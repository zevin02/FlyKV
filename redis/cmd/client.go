package main

import (
	"FlexDB"
	"FlexDB/redis"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberofArry(cmd string) error {
	return fmt.Errorf("ERR wrong number of argument for '%s' command", cmd)
}

type cmdHandler func(cli *FlexClient, args [][]byte) (interface{}, error)

//支持的命令
var supportedCommands = map[string]cmdHandler{
	"set": set,
	"get": get,
}

type FlexClient struct {
	db  *redis.RedisDataStruct
	svr *FlexServer
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

func set(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberofArry("set")
	}
	key, val := args[0], args[1]
	if err := cli.db.Set(key, 0, val); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil

}
func get(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberofArry("set")
	}
	key := args[0]
	res, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(res), nil

}
