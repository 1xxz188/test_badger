package controlEXE

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type ControlEXE struct {
	ctx        context.Context    //信号监听结构体
	cancel     context.CancelFunc //数据产生停止信号
	wgProducer *sync.WaitGroup    //等待生产者协程退出
	wgConsumer *sync.WaitGroup    //等待消费者协程退出
	wgAll      *sync.WaitGroup    //等待所有协程退出

	//统计信息
	wgProducerAddCnt  uint32
	wgProducerDoneCnt uint32
	wgConsumerAddCnt  uint32
	wgConsumerDoneCnt uint32
	wgAllAddCnt       uint32
	wgAllDoneCnt      uint32
}

func CreateControlEXE() *ControlEXE {
	ctx, cancel := context.WithCancel(context.Background())
	return &ControlEXE{
		ctx:        ctx,
		cancel:     cancel,
		wgProducer: new(sync.WaitGroup),
		wgConsumer: new(sync.WaitGroup),
		wgAll:      new(sync.WaitGroup),
	}
}

func (c *ControlEXE) CTXCancel() {
	//fmt.Println("EXE CTXCancel() ...")
	c.cancel()
}
func (c *ControlEXE) CTXDone() <-chan struct{} {
	return c.ctx.Done()
}

func (c *ControlEXE) ProducerAdd(delta int) {
	atomic.AddUint32(&c.wgProducerAddCnt, 1)
	//fmt.Printf("EXE ProducerAdd() delta[%d], wgProducerAddCnt[%d]-wgProducerDoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wgProducerAddCnt), atomic.LoadUint32(&c.wgProducerDoneCnt))
	c.wgProducer.Add(delta)
}

func (c *ControlEXE) ProducerDone() {
	atomic.AddUint32(&c.wgProducerDoneCnt, 1)
	//fmt.Printf("EXE ProducerDone() wgProducerAddCnt[%d]-wgProducerDoneCnt[%d]...\n", atomic.LoadUint32(&c.wgProducerAddCnt), atomic.LoadUint32(&c.wgProducerDoneCnt))
	c.wgProducer.Done()
}

func (c *ControlEXE) ProducerWait() {
	fmt.Println("EXE ProducerWait() ...")
	c.wgProducer.Wait()
}

func (c *ControlEXE) ConsumerAdd(delta int) {
	atomic.AddUint32(&c.wgConsumerAddCnt, 1)
	//fmt.Printf("EXE ConsumerAdd() delta[%d], wgConsumerAddCnt[%d]-wgConsumerDoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wgConsumerAddCnt), atomic.LoadUint32(&c.wgConsumerDoneCnt))
	c.wgConsumer.Add(delta)
}

func (c *ControlEXE) ConsumerDone() {
	atomic.AddUint32(&c.wgConsumerDoneCnt, 1)
	//fmt.Printf("EXE ConsumerDone() wgConsumerAddCnt[%d]-wgConsumerDoneCnt[%d]...\n", atomic.LoadUint32(&c.wgConsumerAddCnt), atomic.LoadUint32(&c.wgConsumerDoneCnt))
	c.wgConsumer.Done()
}

func (c *ControlEXE) ConsumerWait() {
	fmt.Println("EXE ConsumerWait() ...")
	c.wgConsumer.Wait()
}

func (c *ControlEXE) AllAdd(delta int) {
	atomic.AddUint32(&c.wgAllAddCnt, 1)
	//fmt.Printf("EXE AllAdd() delta[%d], wgAllAddCnt[%d]-wgAllDoneCnt[%d]...\n", delta, atomic.LoadUint32(&c.wgAllAddCnt), atomic.LoadUint32(&c.wgAllDoneCnt))
	c.wgAll.Add(delta)
}

func (c *ControlEXE) AllDone() {
	atomic.AddUint32(&c.wgAllDoneCnt, 1)
	//fmt.Printf("EXE AllDone() wgAllAddCnt[%d]-wgAllDoneCnt[%d]...\n", atomic.LoadUint32(&c.wgAllAddCnt), atomic.LoadUint32(&c.wgAllDoneCnt))
	c.wgAll.Done()
}

func (c *ControlEXE) AllWait() {
	fmt.Println("EXE AllWait() ...")
	c.wgAll.Wait()
}
