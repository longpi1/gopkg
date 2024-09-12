// Copyright 2023 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package channel

import (
	"container/list"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultThrottleWindow = time.Millisecond * 100
	defaultMinSize        = 1
)

// item 代表通道中的一个数据项。
type item struct {
	// value 表示数据项的值。
	value interface{}
	// deadline 表示数据项的过期时间。
	deadline time.Time
}

// IsExpired 检查数据项是否已过期。
// 如果 deadline 为零值，则表示数据项未过期。
func (i item) IsExpired() bool {
	if i.deadline.IsZero() {
		return false
	}
	// 如果当前时间晚于 deadline，则表示数据项已过期。
	return time.Now().After(i.deadline)
}

// Option 定义通道的选项类型。
type Option func(c *channel)

// Throttle 定义通道的限流函数类型。
type Throttle func(c Channel) bool

// WithSize 定义通道的大小。如果通道已满，则会阻塞。
// 与 WithNonBlock 选项冲突。
func WithSize(size int) Option {
	return func(c *channel) {
		// 如果是非阻塞模式，则不需要更改大小。
		if size >= defaultMinSize && !c.nonblock {
			c.size = size
		}
	}
}

// WithNonBlock 将通道设置为非阻塞模式。
// 通道在任何情况下都不会阻塞。
func WithNonBlock() Option {
	return func(c *channel) {
		c.nonblock = true
	}
}

// WithTimeout 设置每个通道数据项的过期时间。
// 如果数据项在超时时间内未被消费，则会被丢弃。
func WithTimeout(timeout time.Duration) Option {
	return func(c *channel) {
		c.timeout = timeout
	}
}

// WithTimeoutCallback 设置数据项超时时的回调函数。
func WithTimeoutCallback(timeoutCallback func(interface{})) Option {
	return func(c *channel) {
		c.timeoutCallback = timeoutCallback
	}
}

// WithThrottle 设置生产者和消费者的限流函数。
// 如果生产者限流器触发，则输入通道会被阻塞（如果使用阻塞模式）。
// 如果消费者限流器触发，则输出通道会被阻塞。
func WithThrottle(producerThrottle, consumerThrottle Throttle) Option {
	return func(c *channel) {
		// 如果生产者限流器为空，则设置生产者限流器。
		if c.producerThrottle == nil {
			c.producerThrottle = producerThrottle
		} else {
			// 如果生产者限流器已存在，则将新的限流器与之前的限流器组合，形成一个链式限流器。
			prevChecker := c.producerThrottle
			c.producerThrottle = func(c Channel) bool {
				return prevChecker(c) && producerThrottle(c)
			}
		}
		// 如果消费者限流器为空，则设置消费者限流器。
		if c.consumerThrottle == nil {
			c.consumerThrottle = consumerThrottle
		} else {
			// 如果消费者限流器已存在，则将新的限流器与之前的限流器组合，形成一个链式限流器。
			prevChecker := c.consumerThrottle
			c.consumerThrottle = func(c Channel) bool {
				return prevChecker(c) && consumerThrottle(c)
			}
		}
	}
}

// WithThrottleWindow 设置限流函数检查的时间间隔。
func WithThrottleWindow(window time.Duration) Option {
	return func(c *channel) {
		c.throttleWindow = window
	}
}

// WithRateThrottle 是一个辅助函数，用于控制生产者和消费者的处理速率。
// produceRate 和 consumeRate 表示每秒可以处理多少个数据项，也就是 TPS。
func WithRateThrottle(produceRate, consumeRate int) Option {
	// 限流函数将被顺序调用。
	producedMax := uint64(produceRate)
	consumedMax := uint64(consumeRate)
	var producedBegin, consumedBegin uint64
	var producedTS, consumedTS int64
	return WithThrottle(func(c Channel) bool {
		ts := time.Now().Unix()  // 获取当前秒数。
		produced, _ := c.Stats() // 获取已生产的数据项数量。
		if producedTS != ts {
			// 如果进入新的秒数，则将当前处理的数据项数量作为起始值。
			producedBegin = produced
			producedTS = ts
			return false
		}
		// 获取起始值与当前值的差值。
		producedDiff := produced - producedBegin
		return producedMax > 0 && producedMax < producedDiff
	}, func(c Channel) bool {
		ts := time.Now().Unix() // in second
		_, consumed := c.Stats()
		if consumedTS != ts {
			// move to a new second, so store the current process as beginning value
			consumedBegin = consumed
			consumedTS = ts
			return false
		}
		// get the value of beginning
		consumedDiff := consumed - consumedBegin
		return consumedMax > 0 && consumedMax < consumedDiff
	})
}

var (
	_ Channel = (*channel)(nil)
)

// Channel 提供了一个安全且功能丰富的替代 Go 的 chan struct 接口
type Channel interface {
	// Input 将值发送到 Output 通道。如果通道已关闭，不做任何操作且不会panic
	Input(v interface{})
	// Output 返回一个只读的原生通道给消费者
	Output() <-chan interface{}
	// Len 返回未消费项的数量
	Len() int
	// Stats 返回已生产和已消费的计数
	Stats() (produced uint64, consumed uint64)
	// Close 关闭输出通道。如果通道没有明确关闭，它将在 finalize 时关闭
	Close()
}

// channelWrapper 用于检测用户是否不再持有 Channel 对象的引用，运行时将帮助隐式关闭通道
type channelWrapper struct {
	Channel
}

// channel 实现了一个安全且功能丰富的通道结构，适用于现实世界使用
type channel struct {
	size             int
	state            int32
	consumer         chan interface{}
	nonblock         bool // 非阻塞模式
	timeout          time.Duration
	timeoutCallback  func(interface{})
	producerThrottle Throttle // 假设 Throttle 是一个用于节流的接口或函数类型
	consumerThrottle Throttle
	throttleWindow   time.Duration
	// 统计信息
	produced uint64 // 已经插入到缓冲区的项目
	consumed uint64 // 已经发送到 Output 通道的项目
	// 缓冲区
	buffer     *list.List // TODO：使用高性能队列以减少GC
	bufferCond *sync.Cond
	bufferLock sync.Mutex
}

// New 创建并返回一个新的通道，应用所有提供的选项
func New(opts ...Option) Channel {
	c := new(channel)
	c.size = defaultMinSize
	c.throttleWindow = defaultThrottleWindow
	c.bufferCond = sync.NewCond(&c.bufferLock)
	for _, opt := range opts {
		opt(c) // 应用每个选项来配置通道
	}
	c.consumer = make(chan interface{})
	c.buffer = list.New()
	go c.consume() // 在一个独立的goroutine中开始消费

	// 使用包装器以确保通道在不再被引用时关闭
	cw := &channelWrapper{c}
	runtime.SetFinalizer(cw, func(obj *channelWrapper) {
		obj.Close() // 如果用户已经关闭了通道，再次调用 Close 是安全的
	})
	return cw
}

// Close 安全地关闭通道
func (c *channel) Close() {
	if !atomic.CompareAndSwapInt32(&c.state, 0, -1) {
		return // 如果已经关闭或正在关闭，则返回
	}
	c.bufferCond.Broadcast() // 通知所有等待的goroutine
}

// isClosed 检查通道是否已关闭
func (c *channel) isClosed() bool {
	return atomic.LoadInt32(&c.state) < 0
}

// Input 将一个元素添加到通道中
func (c *channel) Input(v interface{}) {
	if c.isClosed() {
		return // 如果通道已关闭，不添加元素
	}

	// 准备元素，可能带有超时设置
	it := item{value: v}
	if c.timeout > 0 {
		it.deadline = time.Now().Add(c.timeout)
	}

	// 在阻塞模式下检查节流功能
	if !c.nonblock && c.throttling(c.producerThrottle) {
		return
	}

	c.bufferLock.Lock()
	if !c.nonblock {
		// 在阻塞模式下，如果缓冲区已满，则等待
		for c.buffer.Len() >= c.size {
			c.bufferCond.Wait()
			if c.isClosed() {
				c.bufferLock.Unlock()
				return
			}
		}
	}
	c.enqueueBuffer(it)
	atomic.AddUint64(&c.produced, 1)
	c.bufferLock.Unlock()
	c.bufferCond.Signal() // 使用 Signal 因为只有一个goroutine在等待条件
}

// Output 为消费者提供一个只读通道
func (c *channel) Output() <-chan interface{} {
	return c.consumer
}

// Len 返回未消费项的数量
func (c *channel) Len() int {
	produced, consumed := c.Stats()
	return int(produced - consumed)
}

// Stats 方法返回channel中已生产和已消费的消息数量
func (c *channel) Stats() (uint64, uint64) {
	// 使用原子操作加载produced和consumed的值，保证读取的一致性
	produced, consumed := atomic.LoadUint64(&c.produced), atomic.LoadUint64(&c.consumed)
	return produced, consumed
}

// consume 方法用于处理输入缓冲区
func (c *channel) consume() {
	for {
		// 检查是否需要限流
		if c.throttling(c.consumerThrottle) {
			// 如果channel已关闭，则返回
			return
		}

		// 上锁以操作缓冲区
		c.bufferLock.Lock()
		for c.buffer.Len() == 0 {
			if c.isClosed() {
				// 如果channel关闭，关闭消费者通道并更新状态
				close(c.consumer)
				// 使用原子操作将状态设为-2，表示完全关闭
				atomic.StoreInt32(&c.state, -2)
				c.bufferLock.Unlock()
				return
			}
			// 等待条件变量，直到有数据可以消费
			c.bufferCond.Wait()
		}
		// 从缓冲区取出一个元素
		it, ok := c.dequeueBuffer()
		c.bufferLock.Unlock()
		// 唤醒其他等待的goroutine
		c.bufferCond.Broadcast()
		if !ok {
			// 理论上这个情况不会发生，因为之前已经检查过缓冲区是否为空
			continue
		}

		// 检查消息是否过期
		if it.IsExpired() {
			if c.timeoutCallback != nil {
				// 如果有超时回调，则执行回调函数
				c.timeoutCallback(it.value)
			}
			// 增加消费计数
			atomic.AddUint64(&c.consumed, 1)
			continue
		}
		// 发送数据到消费者通道，如果这里阻塞，表示消费者正忙
		c.consumer <- it.value
		// 更新已消费的消息数量
		atomic.AddUint64(&c.consumed, 1)
	}
}

// throttling 方法处理限流逻辑
func (c *channel) throttling(throttle Throttle) (closed bool) {
	if throttle == nil {
		return false // 如果没有设置限流器，直接返回false
	}
	throttled := throttle(c)
	if !throttled {
		return false // 如果不需限流，也直接返回
	}
	ticker := time.NewTicker(c.throttleWindow)
	defer ticker.Stop()

	closed = c.isClosed()
	// 只要需要限流并且channel未关闭，继续等待
	for throttled && !closed {
		<-ticker.C // 等待一个时间窗口
		// 重新检查是否仍然需要限流或channel是否已关闭
		throttled, closed = throttle(c), c.isClosed()
	}
	return closed
}

// enqueueBuffer 将一个item加入到缓冲区的末尾
func (c *channel) enqueueBuffer(it item) {
	c.buffer.PushBack(it)
}

// dequeueBuffer 从缓冲区取出一个item
func (c *channel) dequeueBuffer() (it item, ok bool) {
	bi := c.buffer.Front()
	if bi == nil {
		return it, false
	}
	c.buffer.Remove(bi)

	it = bi.Value.(item)
	return it, true
}
