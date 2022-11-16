package rwwatch

import (
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadWrite(t *testing.T) {
	t.Parallel()
	require.EqualValues(t, 2, getAlign(2))
	require.EqualValues(t, 4, getAlign(3))
	require.EqualValues(t, 4, getAlign(4))
	require.EqualValues(t, 8, getAlign(5))
	require.EqualValues(t, 16, getAlign(15))
	require.EqualValues(t, 32, getAlign(19))

	s, err := New(5)
	require.NoError(t, err)

	err = s.Write("key1", 1, true, func(keyVersion uint32) error {
		require.Equal(t, uint32(1), keyVersion)
		return nil
	})
	require.Equal(t, ErrWatchKeyNotExist, err)

	err = s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
		require.Equal(t, uint32(1), keyVersion)
		return nil
	})
	require.NoError(t, err)

	err = s.Write("key1", 1, true, func(keyVersion uint32) error {
		require.Equal(t, uint32(1), keyVersion)
		return nil
	})
	require.NoError(t, err)

	err = s.Write("key1", 2, true, func(keyVersion uint32) error {
		require.Equal(t, uint32(2), keyVersion)
		return nil
	})
	require.NoError(t, err)

	err = s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
		require.Equal(t, uint32(3), keyVersion)
		return nil
	})
	require.NoError(t, err)
}

func TestRemove(t *testing.T) {
	t.Parallel()
	s, err := New(5)
	require.NoError(t, err)

	err = s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
		require.Equal(t, uint32(1), keyVersion)
		require.Equal(t, true, isNewKey)
		return nil
	})

	err = s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
		require.Equal(t, uint32(1), keyVersion)
		require.Equal(t, false, isNewKey)
		return nil
	})

	err = s.Remove("key1", 2, true)
	require.Equal(t, ErrWatchVersionConflicts, err)

	err = s.Remove("key1", 1, true)
	require.NoError(t, err)

	err = s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
		require.Equal(t, uint32(1), keyVersion)
		require.Equal(t, true, isNewKey)
		return nil
	})
}

// TestWatchGetConcurrency Test for SetIfAbsent but not ok
func TestWatchGetConcurrency(t *testing.T) {
	t.Parallel()
	s, err := New(16)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(100)
	beginChan := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			<-beginChan
			defer wg.Done()
			err := s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
				require.Equal(t, uint32(1), keyVersion)
				return nil
			})
			require.NoError(t, err)
		}()
	}
	time.Sleep(time.Millisecond * 10)
	close(beginChan)
	wg.Wait()
}

// TestWatchUpdateConcurrency Test for KVWatchKey should exist when conflict
func TestWatchUpdateConcurrency(t *testing.T) {
	t.Parallel()
	s, err := New(16)
	require.NoError(t, err)

	err = s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
		require.Equal(t, uint32(1), keyVersion)
		return nil
	})
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(100)
	beginChan := make(chan struct{})
	conflictCnt := int32(0)
	for i := uint32(1); i <= 100; i++ {
		go func(version uint32) {
			<-beginChan
			defer wg.Done()
			for {
				err := s.Write("key1", version, true, func(keyVersion uint32) error {
					require.Equal(t, version, keyVersion)
					return nil
				})
				if err != nil {
					require.Equal(t, ErrWatchVersionConflicts, err)
					atomic.AddInt32(&conflictCnt, 1)
				} else {
					break
				}
			}
		}(i)
	}
	time.Sleep(time.Millisecond * 10)
	close(beginChan)
	wg.Wait()

	t.Log("conflictCnt: ", conflictCnt)
	err = s.Read("key1", func(keyVersion uint32, isNewKey bool) error {
		require.Equal(t, int32(101), int32(keyVersion))
		return nil
	})
	require.NoError(t, err)
}
