package main

import (
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"time"
)

func main() {
	url := kingpin.Flag("url", "server addr").Default("http://127.0.0.1:11002/").String()
	op := kingpin.Flag("op", "[get] [getDB] [getDBRange] [metrics]").Default("get").String()
	v := kingpin.Flag("v", "parameter for url").Default("Role_10001048").String()

	kingpin.HelpFlag.Short('h')
	kingpin.Version("v0.0.1")
	kingpin.Parse()
	reqUrl := *url + *op + "/" + *v
	now := time.Now()
	switch *op {
	case "getDB":
		fallthrough
	case "get":
		DoGet(reqUrl)
	case "getDBRange":
		DoGetRange(reqUrl)
	case "metrics":
		DoMetrics(reqUrl)
	default:
		fmt.Println("unknown op: ", *op)
	}

	fmt.Println("Cost: ", time.Now().Sub(now))
}
