// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package hardware

import (
	"flag"
	syslog "log"
	"runtime"
	"sync"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	icOnce sync.Once
	ic     bool
	icErr  error
)

// Initialize maxprocs
func InitMaxprocs(serverType string, flags *flag.FlagSet) {
	if serverType == typeutil.EmbeddedRole {
		// Initialize maxprocs while discarding log.
		maxprocs.Set(maxprocs.Logger(nil))
	} else {
		// Initialize maxprocs.
		maxprocs.Set(maxprocs.Logger(syslog.Printf))
	}
}

// GetCPUNum returns the count of cpu core.
func GetCPUNum() int {
	//nolint
	cur := runtime.GOMAXPROCS(0)
	if cur <= 0 {
		//nolint
		cur = runtime.NumCPU()
	}
	return cur
}

// GetCPUUsage returns the cpu usage in percentage.
func GetCPUUsage() float64 {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		log.Warn("failed to get cpu usage",
			zap.Error(err))
		return 0
	}

	if len(percents) != 1 {
		log.Warn("something wrong in cpu.Percent, len(percents) must be equal to 1",
			zap.Int("len(percents)", len(percents)))
		return 0
	}

	return percents[0]
}

// GetMemoryCount returns the memory count in bytes.
func GetMemoryCount() uint64 {
	// get host memory by `gopsutil`
	stats, err := mem.VirtualMemory()
	if err != nil {
		log.Warn("failed to get memory count",
			zap.Error(err))
		return 0
	}

	// get container memory by `cgroups`
	limit, err := getContainerMemLimit()
	// in container, return min(hostMem, containerMem)
	if limit > 0 && limit < stats.Total {
		return limit
	}

	if err != nil || limit > stats.Total {
		log.RatedWarn(3600, "failed to get container memory limit",
			zap.Uint64("containerLimit", limit),
			zap.Error(err))
	}
	return stats.Total
}

// GetFreeMemoryCount returns the free memory in bytes.
func GetFreeMemoryCount() uint64 {
	return GetMemoryCount() - GetUsedMemoryCount()
}

// TODO(dragondriver): not accurate to calculate disk usage when we use distributed storage

// GetDiskCount returns the disk count in bytes.
func GetDiskCount() uint64 {
	return 100 * 1024 * 1024
}

// GetDiskUsage returns the disk usage in bytes.
func GetDiskUsage() uint64 {
	return 2 * 1024 * 1024
}

func GetMemoryUseRatio() float64 {
	usedMemory := GetUsedMemoryCount()
	totalMemory := GetMemoryCount()
	if usedMemory > 0 && totalMemory > 0 {
		return float64(usedMemory) / float64(totalMemory)
	}
	return 0
}
