package main

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"test_badger/controlEXE"
	"testing"
	"time"
)

func CreateDefaultOptions() badger.Options {
	dir := "./data"
	opt := badger.DefaultOptions(dir).
		WithCompactL0OnClose(true).  //退出处理LO压缩
		WithDetectConflicts(false).  //禁用版本冲突(由业务层保障)
		WithBlockCacheSize(2 << 30). //如果加密和压缩开启时，需要开启，否则关闭
		WithValueThreshold(65)       // 小值放LSM树，默认1MB
	return opt
}

func BenchmarkSave(b *testing.B) {
	/*intervalV := b.N

	for idx := 0; idx < 10; idx++ {
		go func(idx int) {
			for i := idx * intervalV; i < idx*intervalV+intervalV; i++ {
				key := fmt.Sprintf("key_%d", i)
				value := fmt.Sprintf("value_%d_%d", i, count)
				//fmt.Printf("Set key: %s\n", string(key))
				err := proxy.Set(key, []byte(value))
				if err != nil {
					log.Fatal(err)
				}
				atomic.AddUint32(&setCnt, 1)
			}
		}(idx)
	}
	*/
}

func TestSave(t *testing.T) {
	db, err := badger.Open(CreateDefaultOptions()) //.WithMemTableSize(1024 * 2)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	c := controlEXE.CreateControlEXE()
	proxy, err := CreateDBProxy(db, c)
	require.NoError(t, err)

	data := func(l int) []byte {
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
	}

	txn := db.NewTransaction(false)
	_, err = proxy.Get(txn, "key_1")
	require.NoError(t, err)
	txn.Discard()

	setCnt := uint32(0)
	go func() {
		lastCnt := uint32(0)
		for {
			time.Sleep(time.Second)
			nowCnt := atomic.LoadUint32(&setCnt)
			fmt.Println("qps: ", nowCnt-lastCnt)
			lastCnt = nowCnt
		}
	}()

	intervalV := 200000

	for idx := 0; idx < 10; idx++ {
		c.WGAdd(1)
		go func(c *controlEXE.ControlEXE, idx int) {
			defer c.WGDone()
			ticker := time.NewTicker(time.Millisecond * 10)
			defer ticker.Stop()
			count := 0
			for {
				select {
				case _ = <-ticker.C:
					count++
					now := time.Now()
					for i := idx * intervalV; i < idx*intervalV+intervalV; i++ {
						key := fmt.Sprintf("key_%d", i)
						value := fmt.Sprintf("value_%d_%d", i, count)
						//fmt.Printf("Set key: %s\n", string(key))
						err = proxy.Set(key, []byte(value))
						require.NoError(t, err)
						atomic.AddUint32(&setCnt, 1)
					}
					fmt.Printf("Set cost[%d ms]\n", time.Since(now).Milliseconds())
					if count > 120 {
						c.CTXCancel()
						return
					}
					/*case <-c.CTXDone(): //TODO 不能取消
					if count <= 10 {
					}return*/
				}
			}
		}(c, idx)
	}

	<-c.CTXDone()
	c.WG2Wait()
	fmt.Println("Main Exit...")
	//Print(db)
}

//TODO 模糊测试
func TestCheckDB(t *testing.T) {
	db, err := badger.Open(CreateDefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	now := time.Now()
	txn := db.NewTransaction(false)
	defer txn.Discard()

	for i := 0; i < 1*80000*10; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d_%d", i, 121)
		item, err := txn.Get([]byte(key))
		require.NoError(t, err)
		val, err := item.ValueCopy(nil)
		require.Equal(t, value, string(val))
	}
	fmt.Printf(">[%d ms]\n", time.Since(now).Milliseconds())
}

func TestDBCount(t *testing.T) {
	db, err := badger.Open(CreateDefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	PrintDBCount(db)
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
