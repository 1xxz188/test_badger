package main

import (
	"errors"
	"fmt"
	json "github.com/json-iterator/go"
	"io/ioutil"
	"net/http"
)

type KV struct {
	K         string `json:"key"`
	V         []byte `json:"value"`
	ExpiresAt uint64 `json:"expiresAt"`
	Err       string `json:"error"`
}

type KVList struct {
	Result []KV   `json:"kvList"`
	Err    string `json:"error"`
}

type Metrics struct {
	Warning               string  `json:"warning"`
	BlockCache            string  `json:"blockCache"`
	IndexCache            string  `json:"indexCache"`
	Err                   string  `json:"error"`
	CacheCount            uint64  `json:"cacheCount"`
	CachePenetrateCount   uint64  `json:"cachePenetrateCount"`
	CachePenetrateCntRate float64 `json:"cachePenetrateCntRate"`
}

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

func DoGet(reqUrl string) {
	v, err := HttpGet(reqUrl)
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
	if kv.Err != "" {
		fmt.Println(kv.Err)
		return
	}
	vv := kv.V[0:19]
	fmt.Printf("[%d](%s,%s)\n", len(kv.V), kv.K, string(vv))
}

func DoGetRange(reqUrl string) {
	v, err := HttpGet(reqUrl)
	if err != nil {
		fmt.Println(err)
		return
	}

	var kv KVList
	err = json.Unmarshal(v, &kv)
	if err != nil {
		fmt.Println(err)
		return
	}
	if kv.Err != "" {
		fmt.Println(kv.Err)
		return
	}
	fmt.Printf("%+v\n", kv)
}

func DoMetrics(reqUrl string) {
	v, err := HttpGet(reqUrl)
	if err != nil {
		fmt.Println(err)
		return
	}

	var msg Metrics
	err = json.Unmarshal(v, &msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	if msg.Err != "" {
		fmt.Println(msg.Err)
		return
	}
	fmt.Printf("%+v\n", msg)
}
