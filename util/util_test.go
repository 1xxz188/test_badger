package util

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBytesToUint64(t *testing.T) {
	a := uint64(1234)
	b := Uint64ToBytes(a)
	n := BytesToUint64(b)
	require.Equal(t, a, n)
}
