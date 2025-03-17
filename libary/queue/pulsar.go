package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Pulsar struct {
	Client   pulsar.Client
	Producer pulsar.Producer
	Consumer pulsar.Consumer
	mu       sync.Mutex
}

// NewPulsar 创建一个新的 Pulsar 客户端，并连接到指定的服务 URL。
func NewPulsar(serviceURL string) (*Pulsar, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: serviceURL,
		// 可根据需要添加更多配置，如认证信息等
	})
	if err != nil {
		return nil, fmt.Errorf("无法创建 Pulsar 客户端: %v", err)
	}

	return &Pulsar{
		Client: client,
	}, nil
}

// RegisterConsumer 为指定的主题和订阅注册一个消费者。
func (p *Pulsar) RegisterConsumer(config PulsarConf) (Consumer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Client == nil {
		return nil, fmt.Errorf("Pulsar 客户端尚未初始化")
	}

	consumer, err := p.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            config.Topic,
		SubscriptionName: config.SubscriptionName,
		Type:             pulsar.SubscriptionType(config.Type),
		// 可根据需要添加更多配置选项
	})
	if err != nil {
		return nil, fmt.Errorf("无法创建消费者: %v", err)
	}
	p.Consumer = consumer
	return p, nil
}

// RegisterProducer 为指定的主题注册一个生产者。
func (p *Pulsar) RegisterProducer(config PulsarConf) (Producer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Client == nil {
		return nil, fmt.Errorf("Pulsar 客户端尚未初始化")
	}

	producer, err := p.Client.CreateProducer(pulsar.ProducerOptions{
		Topic: config.Topic,
		// 可根据需要添加更多配置选项
	})
	if err != nil {
		return nil, fmt.Errorf("无法创建生产者: %v", err)
	}
	p.Producer = producer
	return p, nil
}

// SendMsg 发送一个字符串类型的消息。
func (p *Pulsar) SendMsg(ctx context.Context, topic string, body string) (Msg, error) {
	return p.SendByteMsg(ctx, topic, []byte(body))
}

// SendByteMsg 发送一个字节数组类型的消息。
func (p *Pulsar) SendByteMsg(ctx context.Context, topic string, body []byte) (Msg, error) {
	if p.Producer == nil {
		return Msg{}, fmt.Errorf("生产者尚未初始化")
	}

	messageID, err := p.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: body,
	})
	if err != nil {
		return Msg{}, fmt.Errorf("无法发送消息: %v", err)
	}

	msg := Msg{
		RunType:   SendMsg,
		Topic:     topic,
		MsgId:     messageID.String(),
		Body:      body,
		Timestamp: time.Now(),
	}

	return msg, nil
}

// SendDelayMsg 发送一个延迟消息，目前尚未实现。
func (p *Pulsar) SendDelayMsg(ctx context.Context, topic string, body string, delaySecond int64) (Msg, error) {
	// Pulsar 暂不支持延迟消息，或需要通过其他方式实现
	return Msg{}, fmt.Errorf("延迟消息功能尚未实现")
}

// ListenReceiveMsgDo 监听并接收消息，并通过回调函数处理接收到的消息。
func (p *Pulsar) ListenReceiveMsgDo(ctx context.Context, topic string, receiveDo func(msg Msg)) error {
	if p.Consumer == nil {
		return fmt.Errorf("消费者尚未初始化")
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("停止接收消息")
				return
			default:
				data, err := p.Consumer.Receive(ctx)
				if err != nil {
					log.Printf("接收消息出错: %v", err)
					continue
				}
				msg := Msg{
					RunType:   SendMsg,
					Topic:     topic,
					MsgId:     data.ID().String(),
					Body:      data.Payload(),
					Timestamp: time.Now(),
				}

				// 处理消息
				receiveDo(msg)

				// 确认或否认消息
				if err != nil {
					log.Printf("处理消息出错: %v", err)
					p.Consumer.Nack(data)
				} else {
					if err := p.Consumer.Ack(data); err != nil {
						log.Printf("确认消息出错: %v", err)
						p.Consumer.Nack(data)
					}
				}
			}
		}
	}()

	return nil
}

// Close 关闭 Pulsar 客户端及相关资源。
func (p *Pulsar) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Producer != nil {
		if err := p.Producer.Close(); err != nil {
			log.Printf("关闭生产者出错: %v", err)
		}
		p.Producer = nil
	}
	if p.Consumer != nil {
		p.Consumer.Close()
		p.Consumer = nil
	}
	if p.Client != nil {
		p.Client.Close()
		p.Client = nil
	}
}
