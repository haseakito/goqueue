package goqueue

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests that test the entire system end-to-end

func setupIntegrationTest(t *testing.T) (Connection, chan error) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})

	ctx := context.Background()
	client.FlushDB(ctx)

	errChan := make(chan error, 100)
	conn, err := OpenConnectionWithRedisClient("integration-test", client, errChan)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.StopHeartbeat()
		client.FlushDB(ctx)
		client.Close()
		close(errChan)
	})

	return conn, errChan
}

func TestIntegration_BasicWorkflow(t *testing.T) {
	conn, _ := setupIntegrationTest(t)

	// Create queue
	queue, err := conn.OpenQueue("orders")
	require.NoError(t, err)

	// Add jobs
	job1, err := queue.Add("process order #1")
	require.NoError(t, err)

	job2, err := queue.Add("process order #2")
	require.NoError(t, err)

	// Verify jobs are waiting
	waiting, _ := queue.GetWaitingCount()
	assert.Equal(t, 2, waiting)

	// Process jobs
	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	queue.Process(handler, 1)

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Verify completion
	assert.Equal(t, int32(2), atomic.LoadInt32(&processed))

	job1.Refresh()
	job2.Refresh()

	assert.Equal(t, StatusCompleted, job1.GetStatus())
	assert.Equal(t, StatusCompleted, job2.GetStatus())

	queue.StopConsuming()
}

func TestIntegration_MultipleQueues(t *testing.T) {
	conn, _ := setupIntegrationTest(t)

	// Create multiple queues
	emailQueue, _ := conn.OpenQueue("emails")
	smsQueue, _ := conn.OpenQueue("sms")
	notifQueue, _ := conn.OpenQueue("notifications")

	// Add jobs to each queue
	emailQueue.Add("email 1")
	emailQueue.Add("email 2")
	smsQueue.Add("sms 1")
	notifQueue.Add("notification 1")
	notifQueue.Add("notification 2")
	notifQueue.Add("notification 3")

	// Get stats
	queues, _ := conn.GetOpenQueues()
	assert.ElementsMatch(t, []string{"emails", "sms", "notifications"}, queues)

	stats, _ := conn.CollectStats(queues)
	assert.Equal(t, 2, stats.QueueStats["emails"].ReadyCount)
	assert.Equal(t, 1, stats.QueueStats["sms"].ReadyCount)
	assert.Equal(t, 3, stats.QueueStats["notifications"].ReadyCount)
}

func TestIntegration_ErrorHandlingAndRetry(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("retry-queue")

	opts := JobOptions{
		Attempts: 3,
	}
	job, _ := queue.AddWithOptions("flaky-job", opts)

	attempts := int32(0)
	handler := func(j Job) error {
		count := atomic.AddInt32(&attempts, 1)
		if count < 3 {
			return fmt.Errorf("temporary failure")
		}
		return nil
	}

	queue.Process(handler, 1)
	time.Sleep(1500 * time.Millisecond)

	// Should have retried and eventually succeeded
	job.Refresh()
	assert.Equal(t, StatusCompleted, job.GetStatus())
	assert.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(3))

	queue.StopConsuming()
}

func TestIntegration_PauseResume(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("pausable-queue")

	// Add jobs
	queue.Add("job 1")
	queue.Add("job 2")
	queue.Add("job 3")

	// Pause queue
	queue.Pause()

	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	queue.Process(handler, 2)

	// Wait - should not process
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&processed))

	// Resume
	queue.Resume()
	time.Sleep(500 * time.Millisecond)

	// Should now process
	assert.Equal(t, int32(3), atomic.LoadInt32(&processed))

	queue.StopConsuming()
}

func TestIntegration_HighThroughput(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("high-throughput")

	// Add many jobs
	jobCount := 100
	for i := 0; i < jobCount; i++ {
		queue.Add(fmt.Sprintf("job-%d", i))
	}

	waiting, _ := queue.GetWaitingCount()
	assert.Equal(t, jobCount, waiting)

	// Process with multiple workers
	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		time.Sleep(5 * time.Millisecond) // Simulate work
		return nil
	}

	queue.Process(handler, 10)

	// Wait for all jobs to complete
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout: only processed %d/%d jobs", atomic.LoadInt32(&processed), jobCount)
		case <-ticker.C:
			if atomic.LoadInt32(&processed) == int32(jobCount) {
				goto done
			}
		}
	}

done:
	assert.Equal(t, int32(jobCount), atomic.LoadInt32(&processed))
	queue.StopConsuming()
}

func TestIntegration_RateLimiting(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("rate-limited")

	// Set rate limit: 5 jobs per second
	queue.SetRateLimit(5, 1*time.Second)

	// Add 15 jobs
	for i := 0; i < 15; i++ {
		queue.Add(fmt.Sprintf("job-%d", i))
	}

	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	queue.Process(handler, 1)

	// After 1 second, should have ~5 jobs
	time.Sleep(1100 * time.Millisecond)
	count := atomic.LoadInt32(&processed)
	assert.LessOrEqual(t, count, int32(10)) // Allow margin for timing/scheduling variations

	// After 3 seconds total, should have ~15 jobs
	time.Sleep(2 * time.Second)
	count = atomic.LoadInt32(&processed)
	assert.GreaterOrEqual(t, count, int32(13))

	queue.StopConsuming()
}

func TestIntegration_JobProgressTracking(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("progress-queue")

	job, _ := queue.Add("multi-step job")

	handler := func(j Job) error {
		j.UpdateProgress(10)
		time.Sleep(50 * time.Millisecond)
		j.UpdateProgress(30)
		time.Sleep(50 * time.Millisecond)
		j.UpdateProgress(60)
		time.Sleep(50 * time.Millisecond)
		j.UpdateProgress(90)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	queue.Process(handler, 1)
	time.Sleep(500 * time.Millisecond)

	job.Refresh()
	assert.Equal(t, StatusCompleted, job.GetStatus())

	if rj, ok := job.(*redisJob); ok {
		assert.Equal(t, 100, rj.Progress)
	}

	queue.StopConsuming()
}

func TestIntegration_FailedJobsInRejectedQueue(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("failing-queue")

	opts := JobOptions{
		Attempts: 2,
	}

	// Add jobs that will fail
	job1, _ := queue.AddWithOptions("fail-job-1", opts)
	job2, _ := queue.AddWithOptions("fail-job-2", opts)

	handler := func(job Job) error {
		return fmt.Errorf("intentional failure")
	}

	queue.Process(handler, 1)
	time.Sleep(1 * time.Second)

	// Check failed count
	failedCount, _ := queue.GetFailedCount()
	assert.Equal(t, 2, failedCount)

	// Verify job status
	job1.Refresh()
	job2.Refresh()

	assert.Equal(t, StatusFailed, job1.GetStatus())
	assert.Equal(t, StatusFailed, job2.GetStatus())

	queue.StopConsuming()
}

func TestIntegration_MultipleConnections(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer client.Close()

	ctx := context.Background()
	client.FlushDB(ctx)

	errChan1 := make(chan error, 10)
	errChan2 := make(chan error, 10)

	// Create two connections
	conn1, err := OpenConnectionWithRedisClient("conn1", client, errChan1)
	require.NoError(t, err)

	conn2, err := OpenConnectionWithRedisClient("conn2", client, errChan2)
	require.NoError(t, err)

	// Get all connections
	connections, _ := conn1.GetConnections()
	assert.ElementsMatch(t, []string{conn1.GetName(), conn2.GetName()}, connections)

	// Both should have active heartbeats
	assert.NoError(t, conn1.CheckHeartbeat())
	assert.NoError(t, conn2.CheckHeartbeat())

	conn1.StopHeartbeat()
	conn2.StopHeartbeat()

	close(errChan1)
	close(errChan2)
}

func TestIntegration_BulkOperations(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("bulk-ops")

	// Add multiple jobs
	var jobIDs []string
	for i := 0; i < 5; i++ {
		job, _ := queue.Add(fmt.Sprintf("job-%d", i))
		jobIDs = append(jobIDs, job.GetID())
	}

	// Bulk pause
	err := queue.BulkPause(jobIDs[:3])
	assert.NoError(t, err)

	// Verify paused
	for i := 0; i < 3; i++ {
		job, _ := queue.GetJob(jobIDs[i])
		assert.Equal(t, StatusPaused, job.GetStatus())
	}

	// Bulk resume
	err = queue.BulkResume(jobIDs[:3])
	assert.NoError(t, err)

	// Verify resumed
	for i := 0; i < 3; i++ {
		job, _ := queue.GetJob(jobIDs[i])
		assert.Equal(t, StatusWaiting, job.GetStatus())
	}

	// Bulk remove
	err = queue.BulkRemove(jobIDs)
	assert.NoError(t, err)

	waiting, _ := queue.GetWaitingCount()
	assert.Equal(t, 0, waiting)
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	conn, _ := setupIntegrationTest(t)
	queue, _ := conn.OpenQueue("shutdown-test")

	// Add jobs
	for i := 0; i < 10; i++ {
		queue.Add(fmt.Sprintf("job-%d", i))
	}

	processing := int32(0)
	completed := int32(0)

	handler := func(job Job) error {
		atomic.AddInt32(&processing, 1)
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&completed, 1)
		return nil
	}

	queue.Process(handler, 3)
	time.Sleep(50 * time.Millisecond) // Let some jobs start

	// Stop consuming
	err := queue.StopConsuming()
	assert.NoError(t, err)

	// No new jobs should start processing
	processingCount := atomic.LoadInt32(&processing)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, processingCount, atomic.LoadInt32(&processing))
}
