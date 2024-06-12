package redis

import (
	"github.com/go-redis/redis"
	"github.com/longpi1/gopkg/libary/log"
	cuckoo "github.com/seiflotfy/cuckoofilter"
)

type RedisBloomFilter struct {
	client *redis.Client
	filter *cuckoo.Filter
	key    string
}

func NewRedisBloomFilter(client *redis.Client, key string, size uint, hashes int) (*RedisBloomFilter, error) {
	bf := cuckoo.NewFilter(size)
	rb := &RedisBloomFilter{
		client: client,
		filter: bf,
		key:    key,
	}

	err := rb.load()
	if err != nil {
		return nil, err
	}

	return rb, nil
}

func (rb *RedisBloomFilter) load() error {
	data, err := rb.client.HGetAll(rb.key).Result()
	if err != nil {
		return err
	}

	for key, _ := range data {
		flag := rb.filter.InsertUnique([]byte(key))
		if !flag {
			log.Error("插入失败： %v", key)
		}
	}

	return nil
}
