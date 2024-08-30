package utils

import (
	"math/rand"
	"strings"
	"time"
)

func GetRandomExpiration(expiration int) time.Duration {
	return time.Duration(int64(expiration)+rand.Int63n(10)) * time.Second
}

func GetServerAdders(adders string) []string {
	return strings.Split(adders, ",")
}
