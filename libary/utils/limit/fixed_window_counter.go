package limit

import (
	"sync"
	"time"
)

// FixedWindowCounter 固定窗口计数器限流算法。
// mu 用于同步访问，保证并发安全。
// count 记录当前时间窗口内的请求数量。
// limit 是时间窗口内允许的最大请求数量。
// window 记录当前时间窗口的开始时间。
// duration 是时间窗口的持续时间。
type FixedWindowCounter struct {
	limit    int
	count    int
	duration time.Duration
	window   time.Time
	mu       sync.Mutex
}

// NewFixedWindowCounter 构造函数初始化 FixedWindowCounter 实例。
// limit 参数定义了每个时间窗口内允许的请求数量。
// duration 参数定义了时间窗口的大小。
func NewFixedWindowCounter(limit int, duration time.Duration) *FixedWindowCounter {
	return &FixedWindowCounter{
		window:   time.Now(),
		limit:    limit,
		duration: duration,
	}
}

// Allow 方法用于判断当前请求是否被允许。
// 首先通过互斥锁保证方法的原子性。
func (f *FixedWindowCounter) Allow() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now() // 获取当前时间。

	// 如果当前时间超过了窗口的结束时间，重置计数器和窗口开始时间。
	if now.After(f.window.Add(f.duration)) {
		f.count = 0
		f.window = now
	}

	// 如果当前计数小于限制，则增加计数并允许请求。
	if f.count < f.limit {
		f.count++
		return true
	}
	// 如果计数达到限制，则拒绝请求。
	return false
}
