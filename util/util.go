package util

import (
	"bytes"
	"encoding/binary"
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

// Uint64ToBytes 整形转换成字节
func Uint64ToBytes(n uint64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.BigEndian, n)
	if err != nil {
		panic(err)
	}
	return bytesBuffer.Bytes()
}

// BytesToUint64 字节转换成整形
func BytesToUint64(b []byte) uint64 {
	bytesBuffer := bytes.NewBuffer(b)
	var x uint64
	err := binary.Read(bytesBuffer, binary.BigEndian, &x)
	if err != nil {
		panic(err)
	}
	return x
}
