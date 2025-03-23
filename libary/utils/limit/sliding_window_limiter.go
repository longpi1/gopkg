package limit

import (
	"sync"
	"time"
)

// SlidingWindowLimiter 结构体实现滑动窗口限流算法。
type SlidingWindowLimiter struct {
	mutex          sync.Mutex    // 互斥锁，用于保护并发访问计数器。
	counters       []int         // 计数器数组，每个元素代表一个时间段内的请求数。
	limit          int           // 窗口内允许的最大请求数。
	windowStart    time.Time     // 窗口的起始时间。
	windowDuration time.Duration // 窗口的时间长度。
	interval       time.Duration // 每个计数器代表的时间段长度（窗口长度 / 计数器数量）。
}

// NewSlidingWindowLimiter 构造函数初始化 SlidingWindowLimiter 实例。
func NewSlidingWindowLimiter(limit int, windowDuration time.Duration, interval time.Duration) *SlidingWindowLimiter {
	buckets := int(windowDuration / interval) // 计算计数器的数量（桶的数量）。
	return &SlidingWindowLimiter{
		counters:       make([]int, buckets), // 初始化计数器数组。
		limit:          limit,                // 设置窗口内最大请求数。
		windowStart:    time.Now(),           // 设置窗口起始时间为当前时间。
		windowDuration: windowDuration,       // 设置窗口的时间长度。
		interval:       interval,             // 设置每个计数器的时间间隔。
	}
}

// Allow 方法用于判断当前请求是否被允许，并实现滑动窗口的逻辑。
func (s *SlidingWindowLimiter) Allow() bool {
	s.mutex.Lock()         // 加锁，防止并发修改计数器。
	defer s.mutex.Unlock() // 解锁。

	// 检查是否需要滑动窗口 (当前时间 - 窗口开始时间) > 窗口持续时间
	if time.Since(s.windowStart) > s.windowDuration {
		s.slideWindow() // 如果当前时间已经超过了窗口的起始时间加上窗口的持续时间，则滑动窗口。
	}

	now := time.Now() // 获取当前时间

	// 计算当前请求应该落在哪个计数器中。
	// (当前时间纳秒 - 窗口开始时间纳秒) / 每个计数器的时间间隔(纳秒)  % 计数器数量
	index := int((now.UnixNano()-s.windowStart.UnixNano())/s.interval.Nanoseconds()) % len(s.counters)

	// 检查当前计数器是否已达到限制
	if s.counters[index] < s.limit {
		s.counters[index]++
		return true
	}
	return false
}

// slideWindow 方法实现滑动窗口逻辑，移除最旧的时间段并重置计数器。
func (s *SlidingWindowLimiter) slideWindow() {
	// 滑动窗口，忽略最旧的时间段。
	// 将计数器数组的第2个元素到最后一个元素复制到从第1个元素开始的位置。
	copy(s.counters, s.counters[1:])
	// 重置最后一个时间段的计数器 (将最后一个计数器清零)。
	s.counters[len(s.counters)-1] = 0
	// 更新窗口开始时间为当前时间。 这样做的结果是，旧的窗口起始时间被抛弃了。
	// 更精确的做法是 s.windowStart = s.windowStart.Add(s.interval)
	s.windowStart = time.Now()

}
