package main

import (
	"fmt"
	"log"
	"stathat.com/c/consistent"
	"strconv"
)

func main() {
	// 创建一致性哈希实例
	hashRing := consistent.New()

	// 添加节点
	for i := 0; i < 5; i++ {
		node := "node" + strconv.Itoa(i)
		hashRing.Add(node)
	}

	// 模拟100个键的分布情况
	for j := 0; j < 100; j++ {
		key := "key" + strconv.Itoa(j)
		node, err := hashRing.Get(key) //获得对应实例
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Key '%s' maps to node: %s\n", key, node)
	}
}
