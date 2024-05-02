package queue

import (
	"context"
	"sync"

	"github.com/longpi1/gopkg/libary/log"
)

// ConsumerInterface 消费者接口，实现该接口即可加入到消费队列中
type ConsumerInterface interface {
	GetTopic() string                                // 获取消费主题
	Handle(ctx context.Context, msg Msg) (err error) // 处理消息的方法
}

// consumerManager 消费者管理
type consumerManager struct {
	sync.Mutex
	list map[string]ConsumerInterface // 维护的消费者列表
}

var consumers = &consumerManager{
	list: make(map[string]ConsumerInterface),
}

// RegisterConsumer 注册任务到消费者队列
func RegisterConsumer(cs ConsumerInterface) {
	consumers.Lock()
	defer consumers.Unlock()
	topic := cs.GetTopic()
	if _, ok := consumers.list[topic]; ok {
		log.Info("queue.RegisterConsumer topic:%v duplicate registration.", topic)
		return
	}
	consumers.list[topic] = cs
}

// StartConsumersListener 启动所有已注册的消费者监听
func StartConsumersListener(ctx context.Context) {
	for _, c := range consumers.list {
		go func(c ConsumerInterface) {
			consumerListen(ctx, c)
		}(c)
	}
}

// consumerListen 消费者监听
func consumerListen(ctx context.Context, consumer ConsumerInterface) {
	var (
		topic  = consumer.GetTopic()
		c, err = InstanceConsumer()
	)

	if err != nil {
		log.Fatal(ctx, "InstanceConsumer %s err:%+v", topic, err)
		return
	}

	if listenErr := c.ListenReceiveMsgDo(topic, func(msg Msg) {
		err = consumer.Handle(ctx, msg)
		if err != nil {
			log.Error("消费队列：%s 处理失败, err:%+v", topic, err)
		}
	}); listenErr != nil {
		log.Fatal(ctx, "消费队列：%s 监听失败, err:%+v", topic, listenErr)
	}
}
