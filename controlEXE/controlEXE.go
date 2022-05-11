package controlEXE

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type ControlEXE struct {
	ctx    context.Context    //信号监听结构体
	cancel context.CancelFunc //数据产生停止信号
	wg     *sync.WaitGroup    //等待生产者协程退出
	wg2    *sync.WaitGroup    //等待消费者协程退出
	wg3    *sync.WaitGroup    //等待所有协程退出

	//统计信息
	wgAddCnt   uint32
	wgDoneCnt  uint32
	wg2AddCnt  uint32
	wg2DoneCnt uint32
	wg3AddCnt  uint32
	wg3DoneCnt uint32
}

func CreateControlEXE() *ControlEXE {
	ctx, cancel := context.WithCancel(context.Background())
	return &ControlEXE{
		ctx:    ctx,
		cancel: cancel,
		wg:     new(sync.WaitGroup),
		wg2:    new(sync.WaitGroup),
		wg3:    new(sync.WaitGroup),
	}
}

func (c *ControlEXE) CTXCancel() {
	//fmt.Println("EXE CTXCancel() ...")
	c.cancel()
}
func (c *ControlEXE) CTXDone() <-chan struct{} {
	return c.ctx.Done()
}

func (c *ControlEXE) WGAdd(delta int) {
	atomic.AddUint32(&c.wgAddCnt, 1)
	//fmt.Printf("EXE WGAdd() delta[%d], wgAddCnt[%d]-wgDoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wgAddCnt), atomic.LoadUint32(&c.wgDoneCnt))
	c.wg.Add(delta)
}

func (c *ControlEXE) WGDone() {
	atomic.AddUint32(&c.wgDoneCnt, 1)
	//fmt.Printf("EXE WGDone() wgAddCnt[%d]-wgDoneCnt[%d]...\n", atomic.LoadUint32(&c.wgAddCnt), atomic.LoadUint32(&c.wgDoneCnt))
	c.wg.Done()
}

func (c *ControlEXE) WGWait() {
	fmt.Println("EXE WGWait() ...")
	c.wg.Wait()
}

func (c *ControlEXE) WG2Add(delta int) {
	atomic.AddUint32(&c.wg2AddCnt, 1)
	//fmt.Printf("EXE WG2Add() delta[%d], wg2AddCnt[%d]-wg2DoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wg2AddCnt), atomic.LoadUint32(&c.wg2DoneCnt))
	c.wg2.Add(delta)
}

func (c *ControlEXE) WG2Done() {
	atomic.AddUint32(&c.wg2DoneCnt, 1)
	//fmt.Printf("EXE WG2Done() wg2AddCnt[%d]-wg2DoneCnt[%d]...\n", atomic.LoadUint32(&c.wg2AddCnt), atomic.LoadUint32(&c.wg2DoneCnt))
	c.wg2.Done()
}

func (c *ControlEXE) WG2Wait() {
	fmt.Println("EXE WG2Wait() ...")
	c.wg2.Wait()
}

func (c *ControlEXE) WG3Add(delta int) {
	atomic.AddUint32(&c.wg3AddCnt, 1)
	//fmt.Printf("EXE WG3Add() delta[%d], wg3AddCnt[%d]-wg3DoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wg3AddCnt), atomic.LoadUint32(&c.wg3DoneCnt))
	c.wg3.Add(delta)
}

func (c *ControlEXE) WG3Done() {
	atomic.AddUint32(&c.wg3DoneCnt, 1)
	//fmt.Printf("EXE WG3Done() wg3AddCnt[%d]-wg3DoneCnt[%d]...\n", atomic.LoadUint32(&c.wg3AddCnt), atomic.LoadUint32(&c.wg3DoneCnt))
	c.wg3.Done()
}

func (c *ControlEXE) WG3Wait() {
	fmt.Println("EXE WG3Wait() ...")
	c.wg3.Wait()
}
