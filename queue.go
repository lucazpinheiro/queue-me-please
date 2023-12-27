package main

import (
	"errors"
	"strings"

	"github.com/go-redis/redis"
)

type RedisQueue struct {
	redisClient *redis.Client
	Key         string // key of the q
}

func NewRedisQueue(queueKey, redisAddr string) *RedisQueue {
	return &RedisQueue{
		redisClient: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "", // no password set
			DB:       0,  // use default DB
		}),
		Key: queueKey,
	}
}

func (r *RedisQueue) Push(value Task) error {
	return r.redisClient.LPush(r.Key, serialize(value)).Err()
}

func (r *RedisQueue) Pop() (Task, error) {
	s, err := r.redisClient.RPop(r.Key).Result()
	if err != nil {
		return Task{}, err
	}
	task, err := deserialize(s)
	if err != nil {
		return Task{}, err
	}
	return task, nil
}

func (r *RedisQueue) Size() (int64, error) {
	return r.redisClient.LLen(r.Key).Result()
}

func serialize(t Task) string {
	return t.ID + ":" + t.Task
}

func deserialize(s string) (Task, error) {
	sSlice := strings.Split(s, ":")
	if len(sSlice) != 2 {
		return Task{}, errors.New("Invalid task format")
	}
	t := Task{
		ID:   sSlice[0],
		Task: sSlice[1],
	}
	return t, nil
}
