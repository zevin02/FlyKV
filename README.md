# FlexDB

# 什么是 FlexDB 

FlexDB是基于高效BitCask模型的高性能键值(KV)存储引擎并兼容Redis的部分数据结构和协议。它提供了快速可靠的数据检索和存储功能。通过利用bitcask模型的简单性和有效性
，FlexDB确保了高效的读写操作，从而提高了整体性能。它提供了一种简化的方法来存储和访问键值对，使其成为需要快速响应数据访问的场景的绝佳选择。
FlexDB对速度和简单性的关注使其成为在平衡存储成本的同时优先考虑性能的应用程序的有价值的替代方案。



# 特点
- 采用追加写的形式，读写低延迟，高吞吐。
- 支持多种内存索引结构，包括跳表、ARTree、B+树和B树。
- 崩溃恢复快，通过CRC校验确保数据的一致性。
- 支持MVCC的多版本控制，同时支持快照读取。
- 参考LevelDB的WAL格式，将数据文件进行按Block组织，并实现LRU来管理缓存，提供缓存命中率。
- 通过goroutine定期对内存索引和treeIndex中的过期版本进行compact删除。
- 使用Bloom Filter降低查询开销。
- 实现WriteBatch 支持数据的批处理，保证操作的原子性。
- 使用mmap在启动阶段加速读取磁盘文件构建索引，并利用merge阶段生成的hint文件加速内存索引的构建，提高启动的速度。
- 支持Redis的部分数据结构和协议，兼容redis的客户端。


# 架构
FlexDB的结构图
![](https://i.imgur.com/vASDlNt.png)

FlexDB中的WAL的设计
![](https://i.imgur.com/6wijEY8.png)

FlexDB的mvcc的设计
![](https://i.imgur.com/3kYNF4q.png)


# 如何使用FlexDB
~~~go
package main

import (
	"github.com/zevin02/FlexDB"
	"fmt"
)

func main() {
	opts := FlexDB.DefaultOperations
	//启动一个db实例
	db, err := FlexDB.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("lily"))
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("10"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	_, err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("value=%s", string(val))

}

~~~

# 性能测试

~~~go
goos: linux
goarch: amd64
pkg: FlexDB/benchmark
cpu: AMD Ryzen 7 4800H with Radeon Graphics         
Benchmark_Put
Benchmark_Put-16       	   82589	     17503 ns/op	    4761 B/op	      17 allocs/op
Benchmark_Get
Benchmark_Get-16       	 1866775	       617.6 ns/op	     103 B/op	       4 allocs/op
~~~
