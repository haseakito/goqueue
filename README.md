# **GoQueue**

**GoQueue** is a high-performance, Redis-backed job queue library for Go. It's designed to be simple, reliable, and feature-rich for handling background jobs in your applications.

## Features

*   **High Performance:** Built on top of Redis for fast and reliable job processing.
*   **Concurrent Workers:** Process jobs concurrently with multiple goroutines.
*   **Job Priorities:** Assign priorities to jobs to control execution order.
*   **Delayed Jobs:** Schedule jobs to run after a specific delay.
*   **Automatic Retries:** Configure automatic retries for failed jobs.
*   **Job Lifecycle Tracking:** Full visibility into job status (waiting, active, completed, failed, paused).
*   **Progress Reporting:** Update and track the progress of long-running jobs.
*   **Queue Management:** Pause and resume queues, and get real-time statistics.

## Installation

```sh
go get github.com/haseakito/goqueue
```

## Getting Started

Here's a basic example of how to add and process jobs with GoQueue:

```go
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

	// Add a simple job
	job1, err := queue.Add("send email to user@example.com")
	if err != nil {
		log.Fatalf("Failed to add job: %v", err)
	}
	fmt.Printf("Added job: %s", job1.GetID())

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
	fmt.Printf("Added delayed job: %s", job2.GetID())

	// Define a handler to process jobs
	handler := func(job goqueue.Job) error {
		fmt.Printf("Processing job %s: %s", job.GetID(), job.GetData())
		// Simulate work
		time.Sleep(1 * time.Second)
		fmt.Printf("Completed job %s", job.GetID())
		return nil
	}

	// Start processing with 3 concurrent workers
	if err := queue.Process(handler, 3); err != nil {
		log.Fatalf("Failed to start processing: %v", err)
	}

	// Let it process for a while
	time.Sleep(10 * time.Second)

	// Stop processing
	if err := queue.StopConsuming(); err != nil {
		log.Fatalf("Failed to stop consuming: %v", err)
	}

	fmt.Println("Queue processing stopped")
}
```

## Advanced Usage

This example demonstrates more advanced features like job progress updates and retry logic for failed jobs.

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/haseakito/goqueue"
)

// Define a struct for your job payload
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	errChan := make(chan error, 10)
	go func() {
		for err := range errChan {
			log.Printf("Error: %v", err)
		}
	}()

	conn, err := goqueue.OpenConnection("email-service", "tcp", "localhost:6379", 0, errChan)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.StopHeartbeat()

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
		fmt.Printf("Added email job %s for %s", job.GetID(), email.To)
	}

	// Process jobs with retry logic
	handler := func(job goqueue.Job) error {
		var email EmailJob
		if err := json.Unmarshal([]byte(job.GetData()), &email); err != nil {
			return fmt.Errorf("invalid job data: %w", err)
		}

		fmt.Printf("[%s] Sending email to %s...", job.GetID(), email.To)

		// Update progress
		job.UpdateProgress(50)
		time.Sleep(500 * time.Millisecond)

		// Simulate a temporary failure
		if email.To == "user2@example.com" && job.GetAttempts() == 0 {
			return fmt.Errorf("temporary network error")
		}

		job.UpdateProgress(100)
		fmt.Printf("[%s] Email sent to %s successfully", job.GetID(), email.To)
		return nil
	}

	// Start processing with 2 workers
	if err := queue.Process(handler, 2); err != nil {
		log.Fatalf("Failed to start processing: %v", err)
	}

    // ... wait for jobs to complete ...

	queue.StopConsuming()
}
```

## Job Options

You can customize job behavior by providing `JobOptions` when adding a job:

```go
options := goqueue.JobOptions{
    Priority:  10,            // Higher priority jobs are processed first
    Delay:     5 * time.Second, // Delay before the job becomes ready
    Attempts:  3,             // Maximum number of retry attempts
    Timeout:   60 * time.Second, // Job timeout duration
}
job, err := queue.AddWithOptions("my job data", options)
```

## Job Interface

The `Job` interface provides methods to interact with a job during its lifecycle:

```go
type Job interface {
    GetID() string
    GetData() string
    GetStatus() JobStatus
    Refresh() error
    UpdateProgress(progress int) error
    MoveToCompleted(result string) error
    MoveToFailed(reason string) error
    Retry() error
    Remove() error
    GetAttempts() int
    GetTimestamp() time.Time
}
```

## Queue Management

You can manage your queues with the following methods:

- `queue.Pause()`: Pause processing of new jobs in the queue.
- `queue.Resume()`: Resume a paused queue.
- `queue.GetWaitingCount()`: Get the number of jobs waiting to be processed.
- `queue.GetActiveCount()`: Get the number of jobs currently being processed.
- `queue.GetFailedCount()`: Get the number of failed jobs.
- `queue.Empty()`: Remove all jobs from the queue.

## Contributing

Contributions are welcome! Please feel free to submit a pull request.

## License

This project is licensed under the [MIT License](LICENCE).
