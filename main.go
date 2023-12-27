package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/google/uuid"
)

const (
	queueKey  = "queue"
	redisAddr = "localhost:6379"
)

type Task struct {
	ID   string `json:"id"`
	Task string `json:"task"`
}

func delayTask() {
	min := 1000
	max := 5000
	t := rand.Intn(max-min) + min
	time.Sleep(time.Duration(t) * time.Millisecond)
}

func taskConsumer(queue *RedisQueue) {
	for {
		task, err := queue.Pop()
		if err != nil {
			if err.Error() == "redis: nil" {
				continue
			}
			panic(err)
		}
		redisClient := redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		defer redisClient.Close()

		_, err = redisClient.Set(task.ID, task.Task, 0).Result()
		if err != nil {
			log.Print(err)
		}
		log.Printf("Running task: %s", task.Task)

		delayTask()

		_, err = redisClient.Del(task.ID).Result()
		if err != nil {
			log.Print(err)
		}
		log.Printf("Finished task: %s", task.Task)
	}
}

func main() {
	queue := NewRedisQueue(queueKey, redisAddr)
	defer queue.redisClient.Close()

	for i := 0; i < 10; i++ {
		go taskConsumer(queue)
	}

	app := fiber.New()

	app.Use(logger.New())

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})
	app.Get("/size", func(c *fiber.Ctx) error {
		size, err := queue.Size()
		if err != nil {
			return c.SendStatus(fiber.StatusInternalServerError)
		}
		return c.JSON(map[string]int64{"queue size": size})
	})
	// app.Get("/running", func(c *fiber.Ctx) error {
	// 	redisClient := redis.NewClient(&redis.Options{
	// 		Addr:     redisAddr,
	// 		Password: "", // no password set
	// 		DB:       0,  // use default DB
	// 	})
	// 	defer redisClient.Close()

	// 	keys, err := redisClient.Keys("*").Result()
	// 	if err != nil {
	// 		return c.SendStatus(fiber.StatusInternalServerError)
	// 	}
	// 	return c.JSON(map[string][]string{"running tasks": keys})
	// })
	app.Post("/queueme", func(c *fiber.Ctx) error {
		task := new(Task)
		if err := c.BodyParser(task); err != nil {
			fmt.Print(err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		task.ID = uuid.New().String()
		if err := queue.Push(*task); err != nil {
			log.Print(err)
			return c.SendStatus(fiber.StatusInternalServerError)
		}
		return c.SendStatus(fiber.StatusOK)
	})

	app.Listen(":3000")
}
