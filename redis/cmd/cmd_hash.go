package cmd

import (
	"FlexDB/redis/common"
	"github.com/tidwall/redcon"
)

func HSet(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, common.NewWrongNumberofArry("hset")
	}
	key, field, val := args[0], args[1], args[2]
	ok, err := cli.db.HSet(common.EncodeKeyWithIndex(key, cli.dbIndex), field, val)
	if err != nil {
		return nil, err
	}
	var res int = 0
	if ok {
		res = 1
	}
	return redcon.SimpleInt(res), nil
}

func HGet(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, common.NewWrongNumberofArry("hget")
	}
	key, field := args[0], args[1]
	val, err := cli.db.HGet(common.EncodeKeyWithIndex(key, cli.dbIndex), field)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	} else {
		return redcon.SimpleString(val), nil
	}

}

func HDel(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, common.NewWrongNumberofArry("hdel")
	}
	key, field := args[0], args[1]
	ok, err := cli.db.HDel(common.EncodeKeyWithIndex(key, cli.dbIndex), field)
	if err != nil {
		return nil, err
	}
	var res int = 0
	if ok {
		res = 1
	}
	return redcon.SimpleInt(res), nil

}
