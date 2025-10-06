package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/haseakito/goqueue"
)

// EmailJob represents an email job payload
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	// Create error channel
	errChan := make(chan error, 10)
	go func() {
		for err := range errChan {
			log.Printf("Error: %v", err)
		}
	}()

	// Open connection
	conn, err := goqueue.OpenConnection("email-service", "tcp", "localhost:6379", 0, errChan)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.StopHeartbeat()

	// Open queue
	queue, err := conn.OpenQueue("emails")
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}

	// Add some email jobs
	emails := []EmailJob{
		{To: "user1@example.com", Subject: "Welcome", Body: "Welcome to our service!"},
		{To: "user2@example.com", Subject: "Update", Body: "We have an update for you."},
		{To: "user3@example.com", Subject: "Notification", Body: "You have a new notification."},
	}

	for _, email := range emails {
		data, _ := json.Marshal(email)
		job, err := queue.Add(string(data))
		if err != nil {
			log.Printf("Failed to add job: %v", err)
			continue
		}
		fmt.Printf("Added email job %s for %s\n", job.GetID(), email.To)
	}

	// Process jobs with retry logic
	handler := func(job goqueue.Job) error {
		var email EmailJob
		if err := json.Unmarshal([]byte(job.GetData()), &email); err != nil {
			return fmt.Errorf("invalid job data: %w", err)
		}

		fmt.Printf("[%s] Sending email to %s...\n", job.GetID(), email.To)

		// Update progress
		job.UpdateProgress(25)
		time.Sleep(500 * time.Millisecond)

		// Simulate email sending
		job.UpdateProgress(75)
		time.Sleep(500 * time.Millisecond)

		// Simulate random failure for demonstration
		if email.To == "user2@example.com" && job.GetAttempts() == 0 {
			return fmt.Errorf("temporary network error")
		}

		job.UpdateProgress(100)
		fmt.Printf("[%s] Email sent to %s successfully\n", job.GetID(), email.To)
		return nil
	}

	// Start processing with 2 workers
	if err := queue.Process(handler, 2); err != nil {
		log.Fatalf("Failed to start processing: %v", err)
	}

	// Monitor queue stats
	ticker := time.NewTicker(2 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				waiting, _ := queue.GetWaitingCount()
				active, _ := queue.GetActiveCount()
				failed, _ := queue.GetFailedCount()

				fmt.Printf("\n=== Queue Stats ===\n")
				fmt.Printf("Waiting: %d\n", waiting)
				fmt.Printf("Active:  %d\n", active)
				fmt.Printf("Failed:  %d\n", failed)
				fmt.Printf("==================\n\n")

				if waiting == 0 && active == 0 {
					done <- true
					return
				}
			}
		}
	}()

	// Wait for all jobs to complete
	<-done
	ticker.Stop()

	// Stop consuming
	queue.StopConsuming()

	// Get connection info
	fmt.Printf("\nConnection: %s\n", conn.GetName())
	openQueues, _ := conn.GetOpenQueues()
	fmt.Printf("Open queues: %v\n", openQueues)

	// Collect stats for all queues
	stats, _ := conn.CollectStats(openQueues)
	fmt.Printf("\nFinal Stats:\n")
	for queueName, stat := range stats.QueueStats {
		fmt.Printf("  %s: Ready=%d, Unacked=%d, Rejected=%d\n",
			queueName, stat.ReadyCount, stat.UnackedCount, stat.RejectedCount)
	}

	// Clean up old completed jobs
	cleaner := goqueue.NewCleaner(conn.GetRedisClient())
	cleanOptions := goqueue.CleanOptions{
		Status: goqueue.StatusCompleted,
		MaxAge: 1 * time.Minute,
	}
	cleanedCount, err := cleaner.Clean(queue, cleanOptions)
	if err != nil {
		log.Printf("Failed to clean jobs: %v", err)
	} else {
		fmt.Printf("\nCleaned %d completed jobs.\n", cleanedCount)
	}
}
