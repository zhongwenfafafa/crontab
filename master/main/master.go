package main

import (
	"flag"
	"fmt"
	"github.com/zhongwenfafafa/crontab/master"
	"runtime"
	"time"
)

var (
	confFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	// master -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "指定系统配置文件路径")
	flag.Parse()
}

// 初始化线程
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	// 初始化命令行参数
	initArgs()
	// 初始化线程
	initEnv()
	// 加载配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 启动任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 启动Api Http 服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	// 正常退出
	return

ERR:
	fmt.Println(err)
}
