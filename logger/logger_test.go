package logger

import (
	"fmt"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	for i := 0; i < 100; i++ {
		fmt.Printf("test:%d\n", i)
		Log.Infof("test:%d", i)
		Log.Errorf("test:%d", i)
		if i%3 == 0 {
			time.Sleep(time.Second)
		}
	}

}
