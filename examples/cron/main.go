package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/haseakito/goqueue"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// Create error channel
	errChan := make(chan error, 100)
	go func() {
		for err := range errChan {
			log.Printf("Queue error: %v", err)
		}
	}()

	// Open connection
	conn, err := goqueue.OpenConnectionWithRedisClient("cron-example", client, errChan)
	if err != nil {
		log.Fatal("Failed to open connection:", err)
	}
	defer conn.StopHeartbeat()

	// Open queue
	queue, err := conn.OpenQueue("scheduled-tasks")
	if err != nil {
		log.Fatal("Failed to open queue:", err)
	}

	// Create cron scheduler
	scheduler := goqueue.NewCronScheduler(queue)

	// Add cron jobs
	fmt.Println("Setting up cron jobs...")

	// Run every minute
	err = scheduler.AddJob("every-minute", "* * * * *", "Task that runs every minute")
	if err != nil {
		log.Fatal("Failed to add every-minute job:", err)
	}
	fmt.Println("✓ Added job: every-minute (runs every minute)")

	// Run every 5 minutes
	err = scheduler.AddJob("every-5-min", "*/5 * * * *", "Task that runs every 5 minutes")
	if err != nil {
		log.Fatal("Failed to add every-5-min job:", err)
	}
	fmt.Println("✓ Added job: every-5-min (runs every 5 minutes)")

	// Run at 9 AM every weekday
	err = scheduler.AddJob("weekday-morning", "0 9 * * 1-5", "Morning report for weekdays")
	if err != nil {
		log.Fatal("Failed to add weekday-morning job:", err)
	}
	fmt.Println("✓ Added job: weekday-morning (runs at 9 AM on weekdays)")

	// Run at midnight every day
	err = scheduler.AddJob("daily-cleanup", "0 0 * * *", "Daily cleanup task")
	if err != nil {
		log.Fatal("Failed to add daily-cleanup job:", err)
	}
	fmt.Println("✓ Added job: daily-cleanup (runs at midnight)")

	// Add a job with custom options
	options := goqueue.JobOptions{
		Priority: 10,
		Attempts: 5,
		Timeout:  2 * time.Minute,
	}
	err = scheduler.AddJobWithOptions("priority-task", "*/10 * * * *", "High priority task", options)
	if err != nil {
		log.Fatal("Failed to add priority-task job:", err)
	}
	fmt.Println("✓ Added job: priority-task (runs every 10 minutes with high priority)")

	// Start the scheduler
	fmt.Println("\nStarting cron scheduler...")
	err = scheduler.Start()
	if err != nil {
		log.Fatal("Failed to start scheduler:", err)
	}
	fmt.Println("✓ Scheduler started successfully")

	// Display registered jobs
	fmt.Println("\n📋 Registered Cron Jobs:")
	fmt.Println("─────────────────────────────────────────────────────────────")
	jobs := scheduler.GetJobs()
	for _, job := range jobs {
		fmt.Printf("Name:       %s\n", job.Name)
		fmt.Printf("Schedule:   %s\n", job.Expression)
		fmt.Printf("Data:       %s\n", job.Data)
		fmt.Printf("Next Run:   %s\n", job.NextRun.Format("2006-01-02 15:04:05"))
		fmt.Println("─────────────────────────────────────────────────────────────")
	}

	// Process jobs from the queue
	fmt.Println("\n🚀 Starting job processor...")
	processed := 0
	handler := func(job goqueue.Job) error {
		processed++
		fmt.Printf("\n[%s] Processing job #%d\n", time.Now().Format("15:04:05"), processed)
		fmt.Printf("  ID:   %s\n", job.GetID())
		fmt.Printf("  Data: %s\n", job.GetData())

		// Simulate work
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("  ✓ Job completed successfully\n")
		return nil
	}

	err = queue.Process(handler, 3) // Process with 3 concurrent workers
	if err != nil {
		log.Fatal("Failed to start processing:", err)
	}

	fmt.Println("✓ Job processor started with 3 workers")
	fmt.Println("\n⏳ Waiting for scheduled jobs... (Press Ctrl+C to stop)")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	fmt.Println("\n\n🛑 Shutting down...")

	err = scheduler.Stop()
	if err != nil {
		log.Printf("Error stopping scheduler: %v", err)
	}

	err = queue.StopConsuming()
	if err != nil {
		log.Printf("Error stopping queue: %v", err)
	}

	fmt.Printf("\n📊 Total jobs processed: %d\n", processed)
	fmt.Println("✓ Shutdown complete")
}
