package cmd

import (
	"FlexDB/redis/common"
	"github.com/tidwall/redcon"
)

func Set(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, common.NewWrongNumberofArry("set")
	}
	key, val := args[0], args[1]
	//这个地方的key先进行编码结合上dbindex在第一个字节中

	if err := cli.db.Set(common.EncodeKeyWithIndex(key, cli.dbIndex), 0, val); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}
func Get(cli *FlexClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, common.NewWrongNumberofArry("get")
	}
	key := args[0]
	res, err := cli.db.Get(common.EncodeKeyWithIndex(key, cli.dbIndex))
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(res), nil

}
