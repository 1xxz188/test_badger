package cachedb

import (
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestDemo(t *testing.T) {
	db, err := NewBigCache(func(key string, entry []byte, reason RemoveReason) {
		t.Logf("Remove [%s], reason[%d]\n", key, reason)
	})
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	data := func(l int) []byte {
		m := make([]byte, l)
		_, err := rand.Read(m)
		if err != nil {
			panic(err)
		}
		return m
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		//key := "key1"
		err := db.Set(key, data(800))
		require.NoError(t, err)
	}

	cache, err := db.GetCache()
	require.NoError(t, err)
	iterator := cache.Iterator()

	for iterator.SetNext() {
		_, err := iterator.Value()
		if err != nil {
			break
		}
		//fmt.Println(e.Key(), len(e.Value()))
	}
	//fmt.Println(string(entry))
}

func TestRemoveExpired(t *testing.T) {
	onRemove := func(key string, entry []byte, reason bigcache.RemoveReason) {
		if reason != bigcache.Deleted {
			if reason == bigcache.Expired {
				t.Errorf("[%d]Expired OnRemove [%s]\n", reason, key)
				t.FailNow()
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	config := bigcache.DefaultConfig(10 * time.Minute)
	config.HardMaxCacheSize = 1
	config.MaxEntrySize = 1024
	config.MaxEntriesInWindow = 1024
	config.OnRemoveWithReason = onRemove
	cache, err := bigcache.NewBigCache(config)
	require.NoError(t, err)
	defer func() {
		err := cache.Close()
		require.NoError(t, err)
	}()

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
		err := cache.Set(key, data(800))
		require.NoError(t, err)
	}
}

func TestDBInsert(t *testing.T) {
	t.Parallel()
	db, err := NewBigCache(func(key string, entry []byte, reason RemoveReason) {
		t.Logf("Remove [%s], reason[%d]\n", key, reason)
	})
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	key := fmt.Sprintf("key_%d", 1)
	//key := "key1"
	err = db.Set(key, []byte("value"))
	require.NoError(t, err)

	cache, err := db.GetCache()
	require.NoError(t, err)
	err = cache.Append(key, []byte("2value2"))
	require.NoError(t, err)

	v, err := db.Get(key)
	require.NoError(t, err)
	t.Log(string(v))
}
