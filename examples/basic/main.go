package main

import (
	"fmt"
	"log"
	"time"

	"github.com/haseakito/goqueue"
)

func main() {
	// Create an error channel to receive errors from the queue system
	errChan := make(chan error, 10)
	go func() {
		for err := range errChan {
			log.Printf("Queue error: %v", err)
		}
	}()

	// Open a connection to Redis
	conn, err := goqueue.OpenConnection("myapp", "tcp", "localhost:6379", 0, errChan)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.StopHeartbeat()

	// Open a queue
	queue, err := conn.OpenQueue("emails")
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}

	// Set a rate limit of 1 job every 2 seconds
	if err := queue.SetRateLimit(1, 2*time.Second); err != nil {
		log.Fatalf("Failed to set rate limit: %v", err)
	}

	// Add jobs to the queue
	job1, err := queue.Add("send email to user@example.com")
	if err != nil {
		log.Fatalf("Failed to add job: %v", err)
	}
	fmt.Printf("Added job: %s\n", job1.GetID())

	// Add a job with custom options
	options := goqueue.JobOptions{
		Priority: 10,
		Delay:    5 * time.Second,
		Attempts: 5,
		Timeout:  60 * time.Second,
	}
	job2, err := queue.AddWithOptions("send welcome email", options)
	if err != nil {
		log.Fatalf("Failed to add job with options: %v", err)
	}
	fmt.Printf("Added delayed job: %s\n", job2.GetID())

	// Process jobs with a handler
	handler := func(job goqueue.Job) error {
		fmt.Printf("Processing job %s: %s\n", job.GetID(), job.GetData())

		// Update progress
		job.UpdateProgress(50)

		// Simulate work
		time.Sleep(1 * time.Second)

		// Job completed successfully
		fmt.Printf("Completed job %s\n", job.GetID())
		return nil
	}

	// Start processing with 3 concurrent workers
	if err := queue.Process(handler, 3); err != nil {
		log.Fatalf("Failed to start processing: %v", err)
	}

	// Get queue statistics
	waitingCount, _ := queue.GetWaitingCount()
	activeCount, _ := queue.GetActiveCount()
	failedCount, _ := queue.GetFailedCount()

	fmt.Printf("Queue stats - Waiting: %d, Active: %d, Failed: %d\n",
		waitingCount, activeCount, failedCount)

	// Let it process for a while
	time.Sleep(10 * time.Second)

	// Stop processing
	if err := queue.StopConsuming(); err != nil {
		log.Fatalf("Failed to stop consuming: %v", err)
	}

	fmt.Println("Queue processing stopped")
}
