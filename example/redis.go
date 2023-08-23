package main

import (
	"FlexDB/redis/cmd"
)

func main() {

	//初始化redis服务器,设置建立连接的回调函数,断开连接的回调函数，以及客户端进来执行的回调函数
	_, err := cmd.NewFlexServer()
	if err != nil {
		panic(err)
	}

}
