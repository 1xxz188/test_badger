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
	wg     *sync.WaitGroup    //所有协程退出等待 --生产者协程(产生数据)退出标志
	wg2    *sync.WaitGroup    //等待协程退出等待 --消费者协程(数据保存) 退出标志

	//统计信息
	wgAddCnt   uint32
	wgDoneCnt  uint32
	wg2AddCnt  uint32
	wg2DoneCnt uint32
}

func CreateControlEXE() *ControlEXE {
	ctx, cancel := context.WithCancel(context.Background())
	return &ControlEXE{
		ctx:    ctx,
		cancel: cancel,
		wg:     new(sync.WaitGroup),
		wg2:    new(sync.WaitGroup),
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
	fmt.Printf("EXE WGAdd() delta[%d], wgAddCnt[%d]-wgDoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wgAddCnt), atomic.LoadUint32(&c.wgDoneCnt))
	c.wg.Add(delta)
}

func (c *ControlEXE) WGDone() {
	atomic.AddUint32(&c.wgDoneCnt, 1)
	fmt.Printf("EXE WGDone() wgAddCnt[%d]-wgDoneCnt[%d]...\n", atomic.LoadUint32(&c.wgAddCnt), atomic.LoadUint32(&c.wgDoneCnt))
	c.wg.Done()
}

func (c *ControlEXE) WGWait() {
	fmt.Println("EXE WGWait() ...")
	c.wg.Wait()
}

func (c *ControlEXE) WG2Add(delta int) {
	atomic.AddUint32(&c.wg2AddCnt, 1)
	fmt.Printf("EXE WG2Add() delta[%d], wgAddCnt[%d]-wgDoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wg2AddCnt), atomic.LoadUint32(&c.wg2DoneCnt))
	c.wg2.Add(delta)
}

func (c *ControlEXE) WG2Done() {
	atomic.AddUint32(&c.wg2DoneCnt, 1)
	fmt.Printf("EXE WG2Done() wgAddCnt[%d]-wgDoneCnt[%d]...\n", atomic.LoadUint32(&c.wg2AddCnt), atomic.LoadUint32(&c.wg2DoneCnt))
	c.wg2.Done()
}

func (c *ControlEXE) WG2Wait() {
	fmt.Println("EXE WG2Wait() ...")
	c.wg2.Wait()
}
