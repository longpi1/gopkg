package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/longpi1/gopkg/libary/conf"
	"github.com/longpi1/gopkg/libary/utils"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

var (
	Client redis.UniversalClient
	//ErrRedisUnlockFail is redis unlock fail error
	ErrRedisUnlockFail = errors.New("redis unlock fail")
	// ErrRedisCmdNotFound is redis command not found error
	ErrRedisCmdNotFound = errors.New("redis command not found; supports only SET and DELETE")
)

// Cache is the interface of redis cache
type Cache interface {
	Get(ctx context.Context, key string, dst interface{}) (bool, error)
	Exist(ctx context.Context, key string) (bool, error)
	Set(ctx context.Context, key string, val interface{}) error
	BFReserve(ctx context.Context, key string, errorRate float64, capacity int64) error
	BFInsert(ctx context.Context, key string, errorRate float64, capacity int64, items ...interface{}) error
	BFAdd(ctx context.Context, key string, item interface{}) error
	BFExist(ctx context.Context, key string, item interface{}) (bool, error)
	CFReserve(ctx context.Context, key string, capacity int64, bucketSize, maxIterations int) error
	CFAdd(ctx context.Context, key string, item interface{}) error
	CFExist(ctx context.Context, key string, item interface{}) (bool, error)
	CFDel(ctx context.Context, key string, item interface{}) error
	IncrBy(ctx context.Context, key string, val int64) error
	Delete(ctx context.Context, key string) error
	GetMutex(mutexname string) *redsync.Mutex
	ExecPipeLine(ctx context.Context, cmds *[]Cmd) error
	Publish(ctx context.Context, topic string, payload interface{}) error
	TopKAdd(ctx context.Context, topic string, payload interface{}) error
	TopKQuery(ctx context.Context, topic string, payload interface{}) ([]bool, error)
}

// CacheImpl is the redis cache client type
type CacheImpl struct {
	client     redis.UniversalClient
	rs         *redsync.Redsync
	expiration int
}

// OpType is the redis operation type
type OpType int

const (
	// SET represents set operation
	SET OpType = iota
	// DELETE represents delete operation
	DELETE
	// INCRBYX represents incrBy if exists operation
	INCRBYX
)

// RedisPayload is a abstract interface for payload type
type RedisPayload interface {
	Payload()
}

// SetPayload is the payload type of set method
type SetPayload struct {
	RedisPayload
	Key string
	Val interface{}
}

// DeletePayload is the payload type of delete method
type DeletePayload struct {
	RedisPayload
	Key string
}

// IncrByXPayload is the payload type of incrByX method
type IncrByXPayload struct {
	RedisPayload
	Key string
	Val int64
}

// Payload implements abstract interface
func (SetPayload) Payload() {}

// Payload implements abstract interface
func (DeletePayload) Payload() {}

// Payload implements abstract interface
func (IncrByXPayload) Payload() {}

// Cmd represents an operation and its payload
type Cmd struct {
	OpType  OpType
	Payload RedisPayload
}

// PipelineCmd is redis pipeline command type
type PipelineCmd struct {
	OpType OpType
	Cmd    interface{}
}

// GetRedisClient 获取一个 Redis 客户端
func GetRedisClient(config *conf.RedisConfig) (redis.UniversalClient, error) {
	if Client == nil {
		Client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:         utils.GetServerAdders(config.Address),
			Password:      config.Password,
			PoolSize:      config.PoolSize,
			MaxRetries:    config.MaxRetries,
			ReadOnly:      true,
			RouteRandomly: true,
		})
		ctx := context.Background()
		_, err := Client.Ping(ctx).Result()
		if err != nil {
			return nil, err
		}
		_ = redisotel.InstrumentTracing(Client)
	}

	return Client, nil
}

// NewRedisCache is the factory of redis cache
func NewRedisCache(config *conf.RedisConfig, client redis.UniversalClient) Cache {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	return &CacheImpl{
		client:     client,
		rs:         rs,
		expiration: config.ExpirationSeconds,
	}
}

// Get returns true if the key already exists and set dst to the corresponding value
func (rc *CacheImpl) Get(ctx context.Context, key string, dst interface{}) (bool, error) {
	val, err := rc.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		_ = json.Unmarshal([]byte(val), dst)
	}
	return true, nil
}

// Exist checks whether a key exists
func (rc *CacheImpl) Exist(ctx context.Context, key string) (bool, error) {
	numExistKey, err := rc.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	exist := numExistKey == 1
	return exist, nil
}

// Set sets a key-value pair
func (rc *CacheImpl) Set(ctx context.Context, key string, val interface{}) error {
	strVal, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err := rc.client.Set(ctx, key, strVal, utils.GetRandomExpiration(rc.expiration)).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) BFReserve(ctx context.Context, key string, errorRate float64, capacity int64) error {
	if err := rc.client.Do(ctx, "bf.reserve", key, errorRate, capacity).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) BFInsert(ctx context.Context, key string, errorRate float64, capacity int64, items ...interface{}) error {
	args := []interface{}{"bf.insert", key, "capacity", capacity, "error", errorRate, "items"}
	args = append(args, items...)
	if err := rc.client.Do(ctx, args...).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) BFAdd(ctx context.Context, key string, item interface{}) error {
	if err := rc.client.Do(ctx, "bf.add", key, item).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) BFExist(ctx context.Context, key string, item interface{}) (bool, error) {
	res, err := rc.client.Do(ctx, "bf.exists", key, item).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (rc *CacheImpl) CFReserve(ctx context.Context, key string, capacity int64, bucketSize, maxIterations int) error {
	if err := rc.client.Do(ctx, "cf.reserve", key, capacity, "BUCKETSIZE", bucketSize, "MAXITERATIONS", maxIterations).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) CFAdd(ctx context.Context, key string, item interface{}) error {
	if err := rc.client.Do(ctx, "cf.add", key, item).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) CFExist(ctx context.Context, key string, item interface{}) (bool, error) {
	res, err := rc.client.Do(ctx, "cf.exists", key, item).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (rc *CacheImpl) CFDel(ctx context.Context, key string, item interface{}) error {
	if err := rc.client.Do(ctx, "cf.del", key, item).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) IncrBy(ctx context.Context, key string, val int64) error {
	return rc.client.IncrBy(ctx, key, val).Err()
}

// Delete deletes a key
func (rc *CacheImpl) Delete(ctx context.Context, key string) error {
	if err := rc.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	return nil
}

func (rc *CacheImpl) GetMutex(mutexname string) *redsync.Mutex {
	return rc.rs.NewMutex(mutexname, redsync.WithExpiry(5*time.Second))
}

var incrByX = redis.NewScript(`
local exists = redis.call('EXISTS', KEYS[1])
if exists == 1 then
	return redis.call('INCRBY', KEYS[1], ARGV[1])
end
`)

// ExecPipeLine execute the given commands in a pipline
func (rc *CacheImpl) ExecPipeLine(ctx context.Context, cmds *[]Cmd) error {
	pipe := rc.client.Pipeline()
	var pipelineCmds []PipelineCmd
	for _, cmd := range *cmds {
		switch cmd.OpType {
		case SET:
			strVal, err := json.Marshal(cmd.Payload.(SetPayload).Val)
			if err != nil {
				return err
			}
			pipelineCmds = append(pipelineCmds, PipelineCmd{
				OpType: SET,
				Cmd:    pipe.Set(ctx, cmd.Payload.(SetPayload).Key, strVal, utils.GetRandomExpiration(rc.expiration)),
			})
		case DELETE:
			pipelineCmds = append(pipelineCmds, PipelineCmd{
				OpType: DELETE,
				Cmd:    pipe.Del(ctx, cmd.Payload.(DeletePayload).Key),
			})
		case INCRBYX:
			payload := cmd.Payload.(IncrByXPayload)
			pipelineCmds = append(pipelineCmds, PipelineCmd{
				OpType: INCRBYX,
				Cmd:    incrByX.Run(ctx, pipe, []string{payload.Key}, payload.Val),
			})
		default:
			return ErrRedisCmdNotFound
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	for _, executedCmd := range pipelineCmds {
		switch executedCmd.OpType {
		case SET:
			if err := executedCmd.Cmd.(*redis.StatusCmd).Err(); err != nil {
				return err
			}
		case DELETE:
			if err := executedCmd.Cmd.(*redis.IntCmd).Err(); err != nil {
				return err
			}
		case INCRBYX:
			if err := executedCmd.Cmd.(*redis.Cmd).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rc *CacheImpl) Publish(ctx context.Context, topic string, payload interface{}) error {
	strVal, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return rc.client.Publish(ctx, topic, strVal).Err()
}

func (rc *CacheImpl) TopKAdd(ctx context.Context, topic string, payload interface{}) error {
	strVal, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return rc.client.TopKAdd(ctx, topic, strVal).Err()
}

func (rc *CacheImpl) TopKQuery(ctx context.Context, topic string, payload interface{}) ([]bool, error) {
	strVal, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return rc.client.TopKQuery(ctx, topic, strVal).Result()
}
