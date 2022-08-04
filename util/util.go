package util

import (
	"math/rand"
	"os"
	"path/filepath"
	"unsafe"
)

func GetDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size >> 20, err
}
func RemoveDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func StringToByteSlice(s string) []byte {
	tmp1 := (*[2]uintptr)(unsafe.Pointer(&s))
	tmp2 := [3]uintptr{tmp1[0], tmp1[1], tmp1[1]}
	return *(*[]byte)(unsafe.Pointer(&tmp2))
}

func ByteSliceToString(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
}

func RandData(l int) []byte {
	m := make([]byte, l)
	_, err := rand.Read(m)
	if err != nil {
		panic(err)
	}
	return m
}
