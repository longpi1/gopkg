package flow

import (
	"fmt"
	"strings"
	"sync"
)

type DataSet interface {
	Set(key string, data interface{}) DataSet
	Get(key string) (data interface{}, ok bool)
	String() string
}

type FlowDataSet struct {
	data map[string]interface{}
	lock sync.RWMutex
}

func NewDataSet() DataSet {
	return &FlowDataSet{
		data: make(map[string]interface{}),
	}
}

func (dataSet *FlowDataSet) Set(key string, data interface{}) DataSet {
	dataSet.lock.Lock()
	defer dataSet.lock.Unlock()
	dataSet.data[key] = data
	return dataSet
}

func (dataSet *FlowDataSet) Get(key string) (data interface{}, ok bool) {
	dataSet.lock.RLock()
	defer dataSet.lock.RUnlock()
	data, ok = dataSet.data[key]
	return
}

func (dataSet *FlowDataSet) String() string {
	dataSet.lock.RLock()
	defer dataSet.lock.RUnlock()
	result := new(strings.Builder)
	for key, value := range dataSet.data {
		result.WriteString(fmt.Sprintf("key=%s,value=%s", key, value))
	}

	return result.String()
}
