// Package queue
// @Link  https://github.com/bufanyun/hotgo
// @Copyright  Copyright (c) 2023 HotGo CLI
// @Author  Ms <133814250@qq.com>
// @License  https://github.com/bufanyun/hotgo/blob/master/LICENSE
package queue

import (
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/longpi1/gopkg/libary/log"
)

// Push 推送队列
func Push(topic string, data interface{}) (err error) {
	q, err := InstanceProducer()
	if err != nil {
		return
	}
	msg, err := q.SendMsg(topic, gconv.String(data))
	if err != nil {
		log.Error("生产队列：%s 发送失败, err:%+v， msg：%+v", topic, err, msg)
	}
	return
}

// DelayPush 推送延迟队列
func DelayPush(topic string, data interface{}, second int64) (err error) {
	q, err := InstanceProducer()
	if err != nil {
		return
	}
	msg, err := q.SendDelayMsg(topic, gconv.String(data), second)
	if err != nil {
		log.Error("生产队列：%s 延迟发送失败, err:%+v， msg：%+v", topic, err, msg)
	}
	return
}
