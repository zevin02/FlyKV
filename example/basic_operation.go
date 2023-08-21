package main

import (
	"FlexDB"
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
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("value=%s", string(val))

}
