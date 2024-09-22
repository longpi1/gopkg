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

package future

import "go.uber.org/atomic"

// future 接口定义了异步操作的结果类型所需的方法
type future interface {
	wait()         // 等待异步操作完成
	OK() bool      // 检查异步操作是否成功完成
	GetErr() error // 获取异步操作的错误，如果有的话
}

// Future 是异步-等待风格的结果类型。
// 它包含了一个异步任务的结果（或错误）。
// 尝试获得结果（或错误）会阻塞，直到异步任务完成。
type Future[T any] struct {
	Ch    chan struct{} // 用于通知任务完成的通道
	Value T             // 异步操作的结果值
	Err   error         // 异步操作的错误
	done  *atomic.Bool  // 原子操作布尔值，用于标记任务是否完成
}

func NewFuture[T any]() *Future[T] {
	return &Future[T]{
		Ch:   make(chan struct{}),   // 创建一个新的通道
		done: atomic.NewBool(false), // 初始化任务未完成
	}
}

func (future *Future[T]) wait() {
	<-future.Ch // 阻塞，直到从通道接收到完成信号
}

// Await 等待异步任务完成并返回结果和错误。
func (future *Future[T]) Await() (T, error) {
	future.wait()
	return future.Value, future.Err
}

// GetValue 返回异步任务的结果，如果没有结果或发生错误则返回nil。
func (future *Future[T]) GetValue() T {
	<-future.Ch // 等待任务完成
	return future.Value
}

// Done 指示异步任务是否已经完成。
func (future *Future[T]) Done() bool {
	return future.done.Load() // 使用原子操作读取完成状态
}

// OK 如果没有发生错误返回true，否则返回false。
func (future *Future[T]) OK() bool {
	<-future.Ch // 等待任务完成
	return future.Err == nil
}

// GetErr 返回异步任务的错误，如果没有错误则返回nil。
func (future *Future[T]) GetErr() error {
	<-future.Ch // 等待任务完成
	return future.Err
}

// Inner 返回一个只读通道，当异步任务完成时该通道会关闭。
// 如果需要在select语句中等待异步任务，可以使用这个通道。
func (future *Future[T]) Inner() <-chan struct{} {
	return future.Ch
}

// Go 启动一个goroutine来执行函数fn，
// 返回一个包含fn结果的Future。
// 注意：如果你需要限制goroutine数量，请使用Pool。
func Go[T any](fn func() (T, error)) *Future[T] {
	future := NewFuture[T]()
	go func() {
		future.Value, future.Err = fn() // 执行函数并保存结果
		close(future.Ch)                // 关闭通道，表示任务完成
		future.done.Store(true)         // 标记任务已完成
	}()
	return future
}

// AwaitAll 等待多个Future完成，
// 如果没有Future返回错误则返回nil，
// 否则返回这些Future中第一个错误。
func AwaitAll[T future](futures ...T) error {
	for i := range futures {
		if !futures[i].OK() {
			return futures[i].GetErr()
		}
	}
	return nil
}

// BlockOnAll 阻塞直到所有Future完成。
// 返回这些Future中第一个错误。
func BlockOnAll[T future](futures ...T) error {
	var err error
	for i := range futures {
		if e := futures[i].GetErr(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
