package main

import (
	_type "FlexDB/redis/type"
	"github.com/tidwall/redcon"
	"strconv"
)

//禁止使用select(一个进程只有一个db实例,将db的名字可以写进去)
func Select(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberofArry("select")
	}
	index, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, err
	}
	if index >= 16 {
		return nil, _type.ErrDbIndexOut
	}
	cli.mu.Lock()
	defer cli.mu.Unlock()
	cli.dbIndex = byte(index)
	return redcon.SimpleString("OK"), nil

}
