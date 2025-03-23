package limit

import (
	"sync"
	"time"
)

// TokenBucket 令牌桶结构体
type TokenBucket struct {
	capacity     int64      // 桶的容量
	tokens       int64      // 当前令牌数量
	rate         int64      // 令牌生成速率（每秒生成多少令牌）
	lastRefillTs time.Time  // 上次填充令牌的时间
	mu           sync.Mutex // 互斥锁，保证并发安全
}

// NewTokenBucket 创建一个新的令牌桶
func NewTokenBucket(capacity, rate int64) *TokenBucket {
	return &TokenBucket{
		capacity:     capacity,
		tokens:       capacity, // 初始时令牌数量等于容量
		rate:         rate,
		lastRefillTs: time.Now(),
	}
}

// refill 填充令牌
func (tb *TokenBucket) refill() {
	now := time.Now()
	// 计算距离上次填充的时间间隔（秒）
	elapsed := now.Sub(tb.lastRefillTs).Seconds()
	if elapsed > 0 {
		// 根据时间间隔和速率计算新生成的令牌数量
		newTokens := int64(elapsed * float64(tb.rate))
		if newTokens > 0 {
			// 更新令牌数量，但不超过桶的容量
			tb.tokens = min(tb.capacity, tb.tokens+newTokens)
			tb.lastRefillTs = now
		}
	}
}

// Take 尝试获取一个令牌
func (tb *TokenBucket) Take() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// 先填充令牌
	tb.refill()

	// 如果当前有令牌可用，消耗一个令牌并返回 true
	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	// 没有令牌可用，返回 false
	return false
}
