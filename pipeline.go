package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueFull = errors.New("the queue is full !!!")

	defaultBatchSize    = 50
	defaultMinWorkerNum = 2
	defaultMaxWorkerNum = 10
	defaultQsize        = 10000
	defaultPeroid       = time.Duration(1 * time.Second)
)

type mergeOption func(*MergeRequests) error

type MergeRequests struct {
	sync.Mutex

	key       string
	batchSize int
	qsize     int
	peroid    time.Duration
	statsFunc func(Stats)

	queue  chan interface{}
	handle func([]interface{}) error

	// worker num
	warn             int
	minWorkerNum     int
	currentWorkerNum int
	maxWorkerNum     int

	ctx    context.Context
	cancel context.CancelFunc
	exited AtomicBool
}

type Stats struct {
	CurrentWorkerNum int
	Queue            int
}

func SetMinWorkerNum(num int) mergeOption {
	return func(mr *MergeRequests) error {
		if num == 0 {
			panic("minWorkerNum must be greater than 1")
		}
		mr.minWorkerNum = num
		return nil
	}
}

func SetMaxWorkerNum(num int) mergeOption {
	return func(mr *MergeRequests) error {
		mr.maxWorkerNum = num
		return nil
	}
}

func SetBatchSize(limit int) mergeOption {
	return func(mr *MergeRequests) error {
		mr.batchSize = limit
		return nil
	}
}

func SetQueueSize(qsize int) mergeOption {
	return func(mr *MergeRequests) error {
		mr.qsize = qsize
		return nil
	}
}

func SetPeriod(period time.Duration) mergeOption {
	return func(mr *MergeRequests) error {
		mr.peroid = period
		return nil
	}
}

func SetContext(ctx context.Context) mergeOption {
	return func(mr *MergeRequests) error {
		cctx, cancel := context.WithCancel(ctx)
		mr.ctx = cctx
		mr.cancel = cancel
		return nil
	}
}

func SetStatsFunc(caller func(Stats)) mergeOption {
	return func(mr *MergeRequests) error {
		mr.statsFunc = caller
		return nil
	}
}

func NewMergeRequests(key string, handle func([]interface{}) error, options ...mergeOption) *MergeRequests {
	// default config
	mr := &MergeRequests{
		key:          key,
		batchSize:    defaultBatchSize,
		minWorkerNum: defaultMinWorkerNum,
		maxWorkerNum: defaultMaxWorkerNum,
		qsize:        defaultQsize,
		peroid:       defaultPeroid,
		statsFunc:    func(Stats) {},
		handle:       handle,
	}

	// opt call
	for _, op := range options {
		op(mr)
	}

	if mr.maxWorkerNum == 0 {
		mr.maxWorkerNum = mr.minWorkerNum * 2 // double factor
	}
	if mr.minWorkerNum > mr.maxWorkerNum {
		mr.maxWorkerNum = mr.minWorkerNum * 2 // double factor
	}

	if mr.ctx == nil {
		cctx, cancel := context.WithCancel(context.Background())
		mr.ctx = cctx
		mr.cancel = cancel
	}
	mr.queue = make(chan interface{}, mr.qsize)
	return mr
}

func (mr *MergeRequests) isRunning() bool {
	return !mr.exited.Get()
}

func (mr *MergeRequests) Start() {
	if mr.ctx == nil {
		cctx, cancel := context.WithCancel(context.Background())
		mr.ctx = cctx
		mr.cancel = cancel
	}

	mr.currentWorkerNum = mr.minWorkerNum
	for i := 0; i < mr.minWorkerNum; i++ {
		go mr.worker()
	}

	go mr.monitor()
}

func (mr *MergeRequests) Stop() {
	mr.cancel()
	mr.exited.SetTrue()
}

func (mr *MergeRequests) monitor() {
	for {
		if mr.exited.IsTrue() {
			return
		}

		mr.grow()

		mr.statsFunc(Stats{
			CurrentWorkerNum: mr.currentWorkerNum,
			Queue:            len(mr.queue),
		})
		time.Sleep(200 * time.Millisecond)
	}
}

func (mr *MergeRequests) incr() {
	mr.Lock()
	defer mr.Unlock()

	mr.currentWorkerNum++
}

func (mr *MergeRequests) desr() {
	mr.Lock()
	defer mr.Unlock()

	mr.currentWorkerNum--
}

func (mr *MergeRequests) grow() {
	mr.Lock()
	defer mr.Unlock()

	// don't need to do.
	if mr.minWorkerNum == mr.maxWorkerNum {
		return
	}

	// don't need to do.
	if mr.currentWorkerNum >= mr.maxWorkerNum {
		return
	}

	if len(mr.queue) < mr.qsize/5 {
		mr.warn = 0
		return
	}

	// reduce jitter
	if mr.warn < 3 {
		mr.warn++
		return
	}

	step := 3
	count := mr.maxWorkerNum - mr.currentWorkerNum
	bulk := count / step // create step by step
	if bulk > 0 {
		count = bulk
	}

	for i := 0; i < count; i++ {
		mr.currentWorkerNum++
		go func() {
			var (
				idlePeriod = 5 * time.Second

				// avoid to exit often.
				lifetime = time.Now().Add(idlePeriod)
			)

			for {
				if !mr.isRunning() {
					return
				}

				// exit
				if len(mr.queue) == 0 && time.Now().After(lifetime) {
					mr.desr()
					continue
				}

				items := mr.multiPop()
				if len(items) == 0 {
					continue
				}

				mr.handle(items)
				lifetime = time.Now().Add(idlePeriod)
			}
		}()
	}

	mr.warn = 0
}

func (mr *MergeRequests) worker() {
	for {
		if !mr.isRunning() {
			return
		}

		items := mr.multiPop()
		mr.handle(items)
	}
}

func (mr *MergeRequests) multiPop() []interface{} {
	// block pop
	var item interface{}
	select {
	case item = <-mr.queue: // block pop
	}

	// try to get some items quickly
	var items = []interface{}{item}
	for i := 0; i < mr.batchSize-1; i++ {
		vv := mr.fastpop()
		if vv == nil {
			break
		}
		items = append(items, vv)
	}

	if len(items) > mr.batchSize {
		return items
	}

	timer := time.NewTimer(mr.peroid)
	defer timer.Stop()

	// if not enough limit num, continue to pop or cause timeout.
	for {
		select {
		case val := <-mr.queue:
			if val == nil { // invalid object
				return items
			}
			items = append(items, val)

		case <-mr.ctx.Done():
			return items

		case <-timer.C:
			return items
		}

		if len(items) >= mr.batchSize {
			return items
		}
	}
}

func (mr *MergeRequests) fastpop() interface{} {
	select {
	case val := <-mr.queue:
		return val

	default: // no block
	}

	return nil
}

func (mr *MergeRequests) Enqueue(entry interface{}) error {
	select {
	case <-mr.ctx.Done():
		return nil

	case mr.queue <- entry:

	default:
		return ErrQueueFull
	}

	return nil
}

func (mr *MergeRequests) EnqueueBlock(entry interface{}) {
	select {
	case mr.queue <- entry:
	case <-mr.ctx.Done():
	}
}

type AtomicBool struct {
	int32
}

func NewAtomicBool(n bool) AtomicBool {
	if n {
		return AtomicBool{1}
	}
	return AtomicBool{0}
}

func (i *AtomicBool) SetTrue() bool {
	return atomic.CompareAndSwapInt32(&i.int32, 0, 1)
}

func (i *AtomicBool) SetFalse() bool {
	return atomic.CompareAndSwapInt32(&i.int32, 1, 0)
}

func (i *AtomicBool) Set(n bool) {
	if n {
		atomic.StoreInt32(&i.int32, 1)
	} else {
		atomic.StoreInt32(&i.int32, 0)
	}
}

func (i *AtomicBool) IsTrue() bool {
	return atomic.LoadInt32(&i.int32) == 1
}

func (i *AtomicBool) IsFalse() bool {
	return atomic.LoadInt32(&i.int32) == 0
}

func (i *AtomicBool) Get() bool {
	return atomic.LoadInt32(&i.int32) == 1
}

func (i *AtomicBool) CompareAndSwap(o, n bool) bool {
	var old, new int32
	if o {
		old = 1
	}
	if n {
		new = 1
	}
	return atomic.CompareAndSwapInt32(&i.int32, old, new)
}
