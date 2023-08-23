package cmd

import (
	"FlexDB/redis/common"
	_type "FlexDB/redis/type"
	"github.com/tidwall/redcon"
	"strconv"
)

//禁止使用select(一个进程只有一个db实例,将db的名字可以写进去)
func Select(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, common.NewWrongNumberofArry("select")
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

func Del(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, common.NewWrongNumberofArry("del")
	}
	key := args[0]
	ok, err := cli.db.Del(common.EncodeKeyWithIndex(key, cli.dbIndex))
	if err != nil {
		return nil, err
	}
	if ok {
		return redcon.SimpleInt(1), nil
	} else {
		return redcon.SimpleInt(0), nil

	}

}
