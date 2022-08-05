package cachedb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBigCache(t *testing.T) {
	var db Cache
	db, err := NewBigCache(DefaultBigCacheOptions(), func(key string, entry []byte, reason RemoveReason) {
		t.Logf("Remove [%s], reason[%d]\n", key, reason)
	})
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	key := "key1"
	v := []byte("value")
	err = db.Set(key, v)
	require.NoError(t, err)

	require.Equal(t, 1, db.Len())

	v2, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, v, v2)

	err = db.Delete(key)
	require.NoError(t, err)

	require.Equal(t, 0, db.Len())
}
