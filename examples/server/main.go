package main

import (
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"syscall"
	"test_badger/proxy"
	"test_badger/web"
)

func main() {
	kWebAddr := kingpin.Flag("web", "ip:port of web listen addr").Default("0.0.0.0:11002").String()
	kGcRate := kingpin.Flag("gcRate", "gc for value log").Default("0.7").Float64()

	kingpin.HelpFlag.Short('h')
	kingpin.Version("v0.0.1")
	kingpin.Parse()

	proxyDB, err := proxy.CreateDBProxy(proxy.DefaultOptions("./data"))
	if err != nil {
		panic(err)
	}
	defer func() {
		err := proxyDB.Close()
		if err != nil {
			panic(err)
		}
	}()

	//开启GC
	go proxyDB.RunGC(*kGcRate)
	//开启网页查询
	go web.RunWeb(*kWebAddr, proxyDB)

	sg := make(chan os.Signal, 1)
	signal.Notify(sg, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	select {
	case <-sg:
		fmt.Println("the app will shutdown.")
		proxyDB.C.CTXCancel()
	}
}
