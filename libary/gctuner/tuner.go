// Copyright 2022 ByteDance Inc.
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

package gctuner

import (
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"
)

// 设定GC百分比的最大值和最小值
var (
	maxGCPercent uint32 = 500 // 最大GC百分比
	minGCPercent uint32 = 50  // 最小GC百分比
)

// 默认的GC百分比值
var defaultGCPercent uint32 = 100

// init函数在包初始化时执行，用于设置默认的GC百分比
func init() {
	gogcEnv := os.Getenv("GOGC") // 从环境变量中获取GOGC的值,用于控制GC的触发频率。它的值是一个百分比，表示新分配的数据达到上一次GC后存活的数据的多少百分比时，将触发下一次GC。
	// 例如，如果GOGC=100（默认值），那么当新分配的数据达到上一次GC后存活的数据的100%时，将触发下一次GC。如果GOGC=200，那么新分配的数据需要达到存活数据的200%才会触发GC。你可以通过设置GOGC的值来平衡内存使用和GC暂停时间。
	gogc, err := strconv.ParseInt(gogcEnv, 10, 32) // 尝试将其转换为整数
	if err != nil {
		return // 如果转换失败，则保持默认值
	}
	defaultGCPercent = uint32(gogc) // 设置默认GC百分比
}

// Tuning Tuning函数用于设置GC调优器的阈值
// 当设置阈值时，环境变量GOGC将不再生效
// threshold: 如果threshold为0，则禁用调优功能
func Tuning(threshold uint64) {
	// 如果阈值为0且当前有调优器，则停止调优并清空全局调优器
	if threshold <= 0 && globalTuner != nil {
		globalTuner.stop()
		globalTuner = nil
		return
	}

	// 如果当前没有调优器，则创建一个新的调优器
	if globalTuner == nil {
		globalTuner = newTuner(threshold)
		return
	}
	// 否则，设置新的阈值
	globalTuner.setThreshold(threshold)
}

// GetGCPercent 返回当前的GC百分比
func GetGCPercent() uint32 {
	if globalTuner == nil {
		return defaultGCPercent // 如果没有调优器，返回默认GC百分比
	}
	return globalTuner.getGCPercent() // 否则返回调优器的GC百分比
}

// GetMaxGCPercent 返回当前的最大GC百分比
func GetMaxGCPercent() uint32 {
	return atomic.LoadUint32(&maxGCPercent) // 以原子方式读取maxGCPercent
}

// SetMaxGCPercent 设置新的最大GC百分比
func SetMaxGCPercent(n uint32) uint32 {
	return atomic.SwapUint32(&maxGCPercent, n) // 以原子方式交换maxGCPercent
}

// GetMinGCPercent 返回当前的最小GC百分比
func GetMinGCPercent() uint32 {
	return atomic.LoadUint32(&minGCPercent) // 以原子方式读取minGCPercent
}

// SetMinGCPercent 设置新的最小GC百分比
func SetMinGCPercent(n uint32) uint32 {
	return atomic.SwapUint32(&minGCPercent, n) // 以原子方式交换minGCPercent
}

// 仅允许一个GC调优器在一个进程中存在
var globalTuner *tuner = nil

/*
Heap内存结构图解：

	_______________  => limit: 主机/容器内存硬限制

|               |
|---------------| => threshold: 当gc_trigger小于threshold时，增加GCPercent
|               |
|---------------| => gc_trigger: heap_live + heap_live * GCPercent / 100
|               |
|---------------|
|   heap_live   |
|_______________|

Go运行时仅在达到gc_trigger时触发GC，gc_trigger由GCPercent和heap_live决定。
因此我们可以动态调整GCPercent来优化GC性能。
*/
type tuner struct {
	finalizer *finalizer // 调优器的finalizer
	gcPercent uint32     // 当前的GC百分比
	threshold uint64     // 高水位线，单位为字节
}

// tuning函数根据内存使用情况动态调整GC百分比
// Go运行时保证该函数会被串行调用
func (t *tuner) tuning() {
	inuse := readMemoryInuse()    // 读取当前的内存使用情况
	threshold := t.getThreshold() // 获取当前的阈值
	// 如果阈值为0，则停止调优
	if threshold <= 0 {
		return
	}
	// 根据当前内存使用情况和阈值计算并设置新的GC百分比
	t.setGCPercent(calcGCPercent(inuse, threshold))
	return
}

// calcGCPercent 计算新的GC百分比
// 参数:
//   - inuse: 当前正在使用的内存大小（字节数）
//   - threshold: 触发GC的内存阈值
//
// 返回值:
//   - 计算出的GC百分比，范围在[minGCPercent, maxGCPercent]之间
func calcGCPercent(inuse, threshold uint64) uint32 {
	// 如果传入的参数无效（如为0），则返回默认的GC百分比
	if inuse == 0 || threshold == 0 {
		return defaultGCPercent
	}
	// 如果当前使用的内存已经超过或等于阈值，则返回最小的GC百分比以强制进行GC
	if threshold <= inuse {
		return minGCPercent
	}
	// 根据公式计算GC百分比: gcPercent = (threshold - inuse) / inuse * 100
	gcPercent := uint32(math.Floor(float64(threshold-inuse) / float64(inuse) * 100))

	// 如果计算出的GC百分比小于最小值，则返回最小值
	if gcPercent < minGCPercent {
		return minGCPercent
		// 如果计算出的GC百分比大于最大值，则返回最大值
	} else if gcPercent > maxGCPercent {
		return maxGCPercent
	}
	// 返回计算出的GC百分比
	return gcPercent
}

// newTuner 创建一个新的tuner实例，并启动调节器
// 参数:
//   - threshold: 初始的内存阈值
//
// 返回值:
//   - 指向新创建的tuner实例的指针
func newTuner(threshold uint64) *tuner {
	t := &tuner{
		gcPercent: defaultGCPercent, // 初始的GC百分比为默认值
		threshold: threshold,        // 设置初始阈值
	}
	t.finalizer = newFinalizer(t.tuning) // 启动调节器，开始自动调节GC
	return t
}

// stop 停止tuner的调节器
func (t *tuner) stop() {
	t.finalizer.stop()
}

// setThreshold 更新内存阈值
// 参数:
//   - threshold: 新的内存阈值
func (t *tuner) setThreshold(threshold uint64) {
	atomic.StoreUint64(&t.threshold, threshold)
}

// getThreshold 获取当前的内存阈值
// 返回值:
//   - 当前的内存阈值
func (t *tuner) getThreshold() uint64 {
	return atomic.LoadUint64(&t.threshold)
}

// setGCPercent 设置新的GC百分比，并更新运行时的GC百分比
// 参数:
//   - percent: 新的GC百分比
//
// 返回值:
//   - 设置前的GC百分比
func (t *tuner) setGCPercent(percent uint32) uint32 {
	atomic.StoreUint32(&t.gcPercent, percent)
	return uint32(debug.SetGCPercent(int(percent)))
}

// getGCPercent 获取当前的GC百分比
// 返回值:
//   - 当前的GC百分比
func (t *tuner) getGCPercent() uint32 {
	return atomic.LoadUint32(&t.gcPercent)
}
