package limit

import (
	"sync"
	"time"
)

// LeakyBucket 漏桶结构体
type LeakyBucket struct {
	capacity     int64      // 桶的容量
	rate         float64    // 漏水速率（每秒流出的请求数）
	water        int64      // 当前桶中的水量（请求数）
	lastLeakTime time.Time  // 上次漏水的时间
	mutex        sync.Mutex // 锁，用于并发安全
}

// NewLeakyBucket 创建一个新的漏桶
func NewLeakyBucket(capacity int64, rate float64) *LeakyBucket {
	return &LeakyBucket{
		capacity:     capacity,
		rate:         rate,
		water:        0,
		lastLeakTime: time.Now(),
	}
}

// leak 模拟漏水的过程，返回当前水量
func (lb *LeakyBucket) leak() {
	now := time.Now()
	// 计算从上次漏水到现在的时间间隔（秒）
	elapsed := now.Sub(lb.lastLeakTime).Seconds()
	// 计算漏掉的水量
	leaked := int64(elapsed * lb.rate)
	if leaked > 0 {
		// 更新水量
		lb.water = max(0, lb.water-leaked)
		// 更新上次漏水时间
		lb.lastLeakTime = now
	}
}

// Allow 判断是否允许请求通过
func (lb *LeakyBucket) Allow() bool {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	// 先执行漏水操作
	lb.leak()

	// 如果当前水量小于桶的容量，允许请求通过
	if lb.water < lb.capacity {
		lb.water++ // 水量加1
		return true
	}
	return false // 桶已满，拒绝请求
}
