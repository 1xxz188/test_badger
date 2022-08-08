package proxy

import (
	"context"
	"fmt"
	"github.com/golang/groupcache/singleflight"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"strconv"
	"sync"
	"sync/atomic"
	"test_badger/badgerApi"
	"test_badger/cachedb"
	"test_badger/controlEXE"
	"test_badger/util"
	"testing"
	"time"
)

func TestReadDB(t *testing.T) {
	util.RemoveDir("./data")
	defer util.RemoveDir("./data")
	fn1 := func() {
		proxy, err := CreateDBProxy(DefaultOptions("./data"))
		require.NoError(t, err)
		defer func() {
			err := proxy.Close()
			require.NoError(t, err)
		}()

		watchKey := "watchKey1"
		keys := []string{"key1"}
		kvList, version := proxy.Gets(watchKey, keys)
		require.Equal(t, 1, len(kvList))
		require.Equal(t, uint32(1), version)
		for _, kv := range kvList {
			//t.Logf("[%d] %+v\n", i, kv)
			require.Equal(t, cachedb.ErrEntryNotFound, kv.Err)
		}

		kvs := append([]badgerApi.KV(nil), badgerApi.KV{
			K: keys[0],
			V: []byte("v1"),
		})
		err = proxy.SetsByVersion(watchKey, version, kvs)
		require.NoError(t, err)

		kvList, version = proxy.Gets(watchKey, keys)
		require.Equal(t, 1, len(kvList))
		require.Equal(t, uint32(2), version)
		for _, kv := range kvList {
			require.Equal(t, nil, kv.Err)
		}

		kvs[0].V = []byte("v2")
		err = proxy.SetsByVersion(watchKey, version, kvs)
		require.NoError(t, err)

		proxy.C.CTXCancel() //触发退出信号
		<-proxy.C.CTXDone()
		proxy.C.ProducerWait()
		proxy.C.ConsumerWait()
		proxy.C.AllWait()
		fmt.Println("1 Exit...")
	}

	fn2 := func() {
		proxy, err := CreateDBProxy(DefaultOptions("./data"))
		require.NoError(t, err)

		defer func() {
			err := proxy.DB.DropAll()
			require.NoError(t, err)
			err = proxy.Close()
			require.NoError(t, err)
		}()

		watchKey := "watchKey1"
		keys := []string{"key1"}
		kvList, version := proxy.Gets(watchKey, keys)
		require.Equal(t, 1, len(kvList))
		require.Equal(t, uint32(1), version)
		for _, kv := range kvList {
			//t.Logf("[%d] %+v\n", i, kv)
			require.Equal(t, nil, kv.Err)
			require.Equal(t, []byte("v2"), kv.V)
		}

		proxy.C.CTXCancel() //触发退出信号
		<-proxy.C.CTXDone()
		proxy.C.ProducerWait()
		proxy.C.ConsumerWait()
		proxy.C.AllWait()
		fmt.Println("2 Exit...")
	}
	fn1()
	fn2()
}

func TestSave(t *testing.T) {
	util.RemoveDir("./data")
	defer util.RemoveDir("./data")
	proxy, err := CreateDBProxy(DefaultOptions("./data"))
	defer func() {
		err := proxy.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	/*data := func(l int) []byte {
		m := make([]byte, l)
		_, err := rand.Read(m)
		if err != nil {
			panic(err)
		}
		return m
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key_%d", i)
		err = proxy.Set(key, data(800))
		require.NoError(t, err)
	}*/

	var keyList []string
	keyList = append(keyList, "key_1")
	watchKey := "watchKey_1"
	items, _ := proxy.Gets(watchKey, keyList)
	if len(items) != len(keyList) {
		panic("len(items) != len(keyList)")
	}
	for _, item := range items {
		require.Equal(t, true, len(item.Err) != 0)
	}

	setCnt := uint32(0)
	chPrintGo := make(chan struct{})
	go func() {
		lastCnt := uint32(0)
		ticker := time.NewTicker(time.Second)
		proxy.C.AllAdd(1)
		defer func() {
			ticker.Stop()
			proxy.C.AllDone()
		}()

		for {
			select {
			case _ = <-ticker.C:
				nowCnt := atomic.LoadUint32(&setCnt)
				fmt.Println("qps: ", nowCnt-lastCnt)
				lastCnt = nowCnt
			case <-chPrintGo:
				fmt.Println("chPrintGo exit")
				return
			}
		}
	}()

	intervalV := 10000

	for idx := 0; idx < 4; idx++ {
		//4个并发竞争
		for ii := 0; ii < 4; ii++ {
			proxy.C.ProducerAdd(1)
			go func(c *controlEXE.ControlEXE, idx int) {
				defer c.ProducerDone()
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				count := 0
				for {
					select {
					case _ = <-ticker.C:
						count++
						//now := time.Now()
						var kvs []badgerApi.KV

						for i := idx * intervalV; i < idx*intervalV+intervalV; i++ {
							kvs = append(kvs, badgerApi.KV{
								K: fmt.Sprintf("key_%d", i),
								V: []byte(fmt.Sprintf("value_%d_%d", i, count)),
							})
						}
						watchKey := fmt.Sprintf("watchKey_%d", idx)
						err = proxy.watchKeyMgr.Read(watchKey, func(keyVersion uint32) error {
							return nil
						})
						require.NoError(t, err)
						err = proxy.Sets(watchKey, kvs)
						require.NoError(t, err)
						atomic.AddUint32(&setCnt, 1)
						//fmt.Printf("Set cost[%d ms]\n", time.Since(now).Milliseconds())
						//time.Sleep(time.Second)
						if count > 1200 {
							c.CTXCancel()
							return
						}
						/*case <-c.CTXDone(): //TODO 不能取消
						if count <= 10 {
						}return*/
					}
				}
			}(proxy.C, idx)
		}
	}

	<-proxy.C.CTXDone()
	proxy.C.ConsumerWait()
	close(chPrintGo)
	proxy.C.AllWait()
	fmt.Println("Main Exit...")
	//Print(db)
}

func TestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg = new(sync.WaitGroup)
	wg.Add(2)

	// children goroutine
	SubFoo := func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		select {
		case <-ctx.Done():
			// parent完成后，就退出
			fmt.Println("SubFoo exit...")
			return
		}
	}
	SubFoo2 := func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		select {
		case <-ctx.Done():
			// parent完成后，就退出
			fmt.Println("SubFoo2 exit...")
			return
		}
	}
	go SubFoo(ctx, wg)
	go SubFoo2(ctx, wg)
	fmt.Println("Main Ready Exit...")
	cancel()
	wg.Wait()
	fmt.Println("Main Exit...")
}

func TestGoDo(t *testing.T) {
	var g singleflight.Group
	c := make(chan string)
	var calls int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return <-c, nil
	}

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) { // n个协程同时调用了g.Do，fn中的逻辑只会被一个协程执行
			v, err := g.Do("key", fn)
			if err != nil {
				t.Errorf("Do error: %v", err)
			}
			if v.(string) != "bar" {
				t.Errorf("got %q; want %q", v, "bar")
			}
			fmt.Printf("id[%d] %#v\n", id, v)
			wg.Done()
		}(i)
	}
	time.Sleep(100 * time.Millisecond) // let goroutines above block
	c <- "bar"
	wg.Wait()
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}
}

func TestFloat(t *testing.T) {
	a1 := uint64(123456)
	a2 := uint64(1)
	b := float64(a2) / float64(a1)

	v, _ := decimal.NewFromFloat(b).Round(6).Float64()
	value := strconv.FormatFloat(v, 'f', 6, 64)
	fmt.Println(v, value) //9.81 被舍去

	v1, _ := decimal.NewFromFloat(9.824).Round(2).Float64()
	v2, _ := decimal.NewFromFloat(9.826).Round(2).Float64()
	v3, _ := decimal.NewFromFloat(9.8251).Round(2).Float64()
	fmt.Println(v1, v2, v3)
}

func TestByteFmt(t *testing.T) {
	key := "key1"
	v := []byte("you\\0me")
	fmt.Println(fmt.Sprintf("{\"key\": \"%s\",\"value\":\"%s\"}", key, v))
}

type OptF struct {
	fn                func(key string, entry []byte)
	fnRemoveButNotDel *func(key string, entry []byte)
}

func TestFnCallFn(t *testing.T) {
	defaultOptions := func() OptF {
		fnRemoveButNotDel := new(func(key string, entry []byte))
		fn := func(key string, entry []byte) {
			//Call
			require.NotNil(t, *fnRemoveButNotDel)
			(*fnRemoveButNotDel)(key, entry)
		}
		return OptF{
			fn:                fn,
			fnRemoveButNotDel: fnRemoveButNotDel,
		}
	}

	op := defaultOptions()
	*op.fnRemoveButNotDel = func(key string, entry []byte) {
		require.Equal(t, "you", key)
		require.Equal(t, []byte("my"), entry)
	}
	op.fn("you", []byte("my"))
}
