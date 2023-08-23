package cmd

//支持的命令
var supportedCommands = map[string]cmdHandler{
	//=============generic====================

	"select": Select,
	//=============string==========================
	"set": Set,
	"get": Get,

	//============hash=============================
	"hset": HSet,
	"hget": HGet,
	"hdel": HDel,
}
