// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pool

import (
	"fmt"
	"strconv"
	_ "strconv"
	"sync"

	ants "github.com/panjf2000/ants/v2"

	"github.com/longpi1/gopkg/libary/future"
	"github.com/longpi1/gopkg/libary/generic"
	"github.com/longpi1/gopkg/libary/hardware"
)

// A goroutine pool
type Pool[T any] struct {
	inner *ants.Pool  // 使用ants包中的Pool来管理协程
	opt   *poolOption // 池的配置选项
}

// NewPool 返回一个新的协程池。
// cap: worker协程的数量。
// 如果提供了任何无效的选项，该函数会panic。
func NewPool[T any](cap int, opts ...PoolOption) *Pool[T] {
	opt := defaultPoolOption() // 获取默认的选项配置
	for _, o := range opts {
		o(opt) // 应用所有提供的选项
	}

	// 使用ants包创建一个新的协程池
	pool, err := ants.NewPool(cap, opt.antsOptions()...)
	if err != nil {
		panic(err) // 如果创建失败，抛出panic
	}

	return &Pool[T]{
		inner: pool,
		opt:   opt,
	}
}

// NewDefaultPool 返回一个默认配置的池，其worker数量等于CPU逻辑核心数，
// 并且预分配协程。
func NewDefaultPool[T any]() *Pool[T] {
	return NewPool[T](hardware.GetCPUNum(), WithPreAlloc(true))
}

// Submit 将一个任务提交到池中并异步执行。
// 如果池的worker数量有限且没有空闲worker，该方法将阻塞。
// 注意：由于当前Go不支持泛型成员方法，我们使用Future[any]
func (pool *Pool[T]) Submit(method func() (T, error)) *future.Future[T] {
	future := future.NewFuture[T]()
	err := pool.inner.Submit(func() {
		defer close(future.Ch) // 确保任务完成后关闭通道
		defer func() {
			if x := recover(); x != nil {
				future.Err = fmt.Errorf("panicked with error: %v", x)
				panic(x) // 将panic重新抛出以获取堆栈跟踪
			}
		}()
		// 执行预处理器
		if pool.opt.preHandler != nil {
			pool.opt.preHandler()
		}
		res, err := method()
		if err != nil {
			future.Err = err
		}
		future.Value = res
	})
	if err != nil {
		future.Err = err
		close(future.Ch)
	}

	return future
}

// Cap 返回工作者的数量
func (pool *Pool[T]) Cap() int {
	return pool.inner.Cap()
}

// Running 返回当前正在运行的工作者的数量
func (pool *Pool[T]) Running() int {
	return pool.inner.Running()
}

// Free 返回空闲工作者的数量
func (pool *Pool[T]) Free() int {
	return pool.inner.Free()
}

// Release 释放池中所有工作者，停止所有的协程。
func (pool *Pool[T]) Release() {
	pool.inner.Release()
}

// Resize 调整池中工作者的数量。
// 如果预分配工作者或提供的尺寸无效，会返回错误。
func (pool *Pool[T]) Resize(size int) error {
	if pool.opt.preAlloc {
		return fmt.Errorf("cannot resize pre-alloc pool")
	}
	if size <= 0 {
		return fmt.Errorf("positive size", strconv.FormatInt(int64(size), 10))
	}
	pool.inner.Tune(size)
	return nil
}

// WarmupPool 对池中的每个协程执行预热逻辑
func WarmupPool[T any](pool *Pool[T], warmup func()) {
	cap := pool.Cap()
	ch := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(cap)
	for i := 0; i < cap; i++ {
		pool.Submit(func() (T, error) {
			warmup() // 执行预热函数
			wg.Done()
			<-ch                          // 等待，直到所有预热完成
			return generic.Zero[T](), nil // 返回T类型的零值
		})
	}
	wg.Wait()
	close(ch)
}
