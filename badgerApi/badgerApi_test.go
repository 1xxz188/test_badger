package badgerApi

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"os"
	"test_badger/util"
	"testing"
)

func TestOpen(t *testing.T) {
	srcDir, err := os.MkdirTemp("./", "badger-test")
	require.NoError(t, err)
	defer util.RemoveDir(srcDir)

	db, err := badger.Open(DefaultOptions(srcDir))
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	dirSize, err := util.GetDirSize(srcDir)
	t.Logf("dir[%s] [%d MB]\n", srcDir, dirSize)
}
