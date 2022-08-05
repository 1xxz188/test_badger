package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"net/http"
	"time"
)

func HttpGet(url string) ([]byte, error) {
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	robots, err := ioutil.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("res.StatusCode: %d", res.StatusCode))
	}
	return robots, nil
}

type KV struct {
	K         string `json:"key"`
	V         []byte `json:"value"`
	ExpiresAt uint64 `json:"expiresAt"`
	Err       error  `json:"error"`
}

func main() {
	url := kingpin.Flag("url", "server addr").Default("http://127.0.0.1:11001/").String()
	op := kingpin.Flag("op", "[get] [getDB] [getDBRange] [metrics]").Default("get").String()
	v := kingpin.Flag("v", "parameter for url").Default("Role_10001048").String()

	kingpin.HelpFlag.Short('h')
	kingpin.Version("v0.0.1")
	kingpin.Parse()

	now := time.Now()
	switch *op {
	case "getDB":
		fallthrough
	case "get":
		v, err := HttpGet(*url + *op + "/" + *v)
		if err != nil {
			fmt.Println(err)
			return
		}

		var kv KV
		err = json.Unmarshal(v, &kv)
		if err != nil {
			fmt.Println(err)
			return
		}
		if kv.Err != nil {
			fmt.Println(kv.Err)
			return
		}
		fmt.Printf("%s, %s\n", kv.K, string(kv.V))
	default:
		fmt.Println("unknown op: ", *op)
	}

	fmt.Println("Cost: ", time.Now().Sub(now))
}
