package main

import (
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type NotificationService struct {
	kafkaProducer sarama.SyncProducer
	kafkaConsumer sarama.ConsumerGroup
	workerPool    *WorkerPool
}

type WorkerPool struct {
	maxWorkers int
	jobs       chan NotificationJob
	wg         sync.WaitGroup
}

type NotificationJob struct {
	CustomerID string
	Message    string
}

func (np *NotificationService) ProcessNotifications() {
	// Consume messages from Kafka queue
	for job := range np.workerPool.jobs {
		np.workerPool.wg.Add(1)
		go func(job NotificationJob) {
			defer np.workerPool.wg.Done()
			np.sendNotification(job)
		}(job)
	}
}

func (np *NotificationService) sendNotification(job NotificationJob) {
	// Logic gửi notification
	// Có thể là push notification, SMS, email, v.v.
	log.Printf("Sending notification to customer %s", job.CustomerID)
}

func NewWorkerPool(maxWorkers int) *WorkerPool {
	return &WorkerPool{
		maxWorkers: maxWorkers,
		jobs:       make(chan NotificationJob, 1000),
	}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.maxWorkers; i++ {
		go func() {
			for job := range wp.jobs {
				// Xử lý job
				wp.processJob(job)
			}
		}()
	}
}

func (wp *WorkerPool) processJob(job NotificationJob) {
	// Xử lý notification job
	println(job.CustomerID, job.Message)
}

func main() {
	// Khởi tạo Kafka producer/consumer
	// Khởi tạo worker pool
	// Bắt đầu xử lý notifications
}
