package queue

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/longpi1/gopkg/libary/constant"
)

type Queue interface {
	Start() error

	Stop()
}

type Producer interface {
	SendMsg(topic string, body string) (msg Msg, err error)
	SendByteMsg(topic string, body []byte) (msg Msg, err error)
	SendDelayMsg(topic string, body string, delaySecond int64) (mqMsg Msg, err error)
}

type Consumer interface {
	ListenReceiveMsgDo(topic string, receiveDo func(Msg Msg)) (err error)
}

const (
	_ = iota
	SendMsg
	ReceiveMsg
)

type Config struct {
	Switch    bool   `json:"switch"`
	Driver    string `json:"driver"`
	Retry     int    `json:"retry"`
	GroupName string `json:"groupName"`
	Rocket    RocketConf
	Kafka     KafkaConf
	Pulsar    PulsarConf
}

type RedisConf struct {
	Timeout int64 `json:"timeout"`
}
type RocketConf struct {
	Address  []string `json:"address"`
	LogLevel string   `json:"logLevel"`
}

type PulsarConf struct {
	Address          []string `json:"address"`
	LogLevel         string   `json:"logLevel"`
	Topic            string   `json:"topic"`
	URL              string   `json:"url"`
	Type             int      `json:"type"`
	SubscriptionName string   `json:"subscriptionName"`
}

type KafkaConf struct {
	Address       []string `json:"address"`
	Version       string   `json:"version"`
	RandClient    bool     `json:"randClient"`
	MultiConsumer bool     `json:"multiConsumer"`
}

type Msg struct {
	RunType   int       `json:"run_type"`
	Topic     string    `json:"topic"`
	MsgId     string    `json:"msg_id"`
	Offset    int64     `json:"offset"`
	Partition int32     `json:"partition"`
	Timestamp time.Time `json:"timestamp"`
	Body      []byte    `json:"body"`
}

var (
	mutex sync.Mutex
)

// InstanceConsumer 实例化消费者
func InstanceConsumer(cfg Config) (mqClient Consumer, err error) {
	return NewConsumer(cfg)
}

// InstanceProducer 实例化生产者
func InstanceProducer(cfg Config) (client Producer, err error) {
	return NewProducer(cfg)
}

// NewProducer 初始化生产者实例
func NewProducer(cfg Config) (client Producer, err error) {
	if cfg.GroupName == "" {
		err = fmt.Errorf("mq groupName is empty")
		return
	}

	switch cfg.Driver {
	case constant.RocketMqName:
		if len(cfg.Rocket.Address) == 0 {
			err = fmt.Errorf("queue rocketmq address is not support")
			return
		}
		client, err = RegisterRocketProducer(cfg.Rocket.Address, cfg.GroupName, cfg.Retry)
	case constant.KafkaMqName:
		if len(cfg.Kafka.Address) == 0 {
			err = fmt.Errorf("queue kafka address is not support")
			return
		}
		client, err = RegisterKafkaProducer(KafkaConfig{
			Brokers: cfg.Kafka.Address,
			GroupID: cfg.GroupName,
			Version: cfg.Kafka.Version,
		})
	case constant.PulsarMqName:
		if len(cfg.Pulsar.Address) == 0 {
			err = fmt.Errorf("queue pulsar address is not support")
			return
		}
		client, err = RegisterPulsarProducer(cfg.Pulsar)
	default:
		err = fmt.Errorf("queue driver is not support")
	}

	if err != nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	return
}

// NewConsumer 初始化消费者实例
func NewConsumer(cfg Config) (client Consumer, err error) {
	if cfg.GroupName == "" {
		err = fmt.Errorf("mq groupName is empty")
		return
	}

	switch cfg.Driver {
	case constant.RocketMqName:
		if len(cfg.Rocket.Address) == 0 {
			err = fmt.Errorf("queue.rocketmq.address is empty")
			return
		}
		client, err = RegisterRocketConsumer(cfg.Rocket.Address, cfg.GroupName)
	case constant.KafkaMqName:
		if len(cfg.Kafka.Address) == 0 {
			err = fmt.Errorf("queue kafka address is not support")
			return
		}

		randTag := strconv.FormatInt(time.Now().Unix(), 10)
		// 是否支持创建多个消费者
		if !cfg.Kafka.MultiConsumer {
			randTag = "001"
		}

		clientId := "Consumer-" + cfg.GroupName
		if cfg.Kafka.RandClient {
			clientId += "-" + randTag
		}

		client, err = RegisterKafkaConsumer(KafkaConfig{
			Brokers:  cfg.Kafka.Address,
			GroupID:  cfg.GroupName,
			Version:  cfg.Kafka.Version,
			ClientId: clientId,
		})
	case constant.PulsarMqName:
		if len(cfg.Pulsar.Address) == 0 {
			err = fmt.Errorf("queue pulsar address is not support")
			return
		}
		client, err = RegisterPulsarConsumer(cfg.Pulsar)
	default:
		err = fmt.Errorf("queue driver is not support")
	}

	if err != nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()
	return
}

// BodyString 返回消息体
func (m *Msg) BodyString() string {
	return string(m.Body)
}

func getRandMsgId() string {
	rand.NewSource(time.Now().UnixNano())
	radium := rand.Intn(999) + 1
	timeCode := time.Now().UnixNano()
	return fmt.Sprintf("%d%.4d", timeCode, radium)
}
