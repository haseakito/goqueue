package goqueue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueue_Process(t *testing.T) {
	queue := setupTestQueue(t)

	// Add jobs
	queue.Add("job1")
	queue.Add("job2")
	queue.Add("job3")

	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	err := queue.Process(handler, 2)
	require.NoError(t, err)

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, int32(3), atomic.LoadInt32(&processed))

	queue.StopConsuming()
}

func TestQueue_Process_WithErrors(t *testing.T) {
	queue := setupTestQueue(t)

	// Add jobs
	job1, _ := queue.Add("success")
	job2, _ := queue.Add("fail")
	job3, _ := queue.Add("success")

	handler := func(job Job) error {
		if job.GetData() == "fail" {
			return fmt.Errorf("intentional error")
		}
		return nil
	}

	queue.Process(handler, 1)

	// Wait for processing
	time.Sleep(1 * time.Second)

	// Check results
	job1.Refresh()
	job2.Refresh()
	job3.Refresh()

	assert.Equal(t, StatusCompleted, job1.GetStatus())
	assert.Equal(t, StatusFailed, job2.GetStatus())
	assert.Equal(t, StatusCompleted, job3.GetStatus())

	queue.StopConsuming()
}

func TestQueue_Process_WithRetry(t *testing.T) {
	queue := setupTestQueue(t)

	opts := JobOptions{
		Attempts: 3,
	}
	queue.AddWithOptions("retry-job", opts)

	attempts := int32(0)
	handler := func(job Job) error {
		count := atomic.AddInt32(&attempts, 1)
		if count < 2 {
			return fmt.Errorf("temporary error")
		}
		return nil
	}

	queue.Process(handler, 1)

	// Wait for retries
	time.Sleep(1 * time.Second)

	// Should have retried once
	assert.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(2))

	queue.StopConsuming()
}

func TestQueue_Pause_Resume(t *testing.T) {
	queue := setupTestQueue(t)

	// Pause the queue
	err := queue.Pause()
	assert.NoError(t, err)
	assert.True(t, queue.IsPaused())

	// Add jobs while paused
	queue.Add("job1")
	queue.Add("job2")

	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	queue.Process(handler, 1)

	// Wait a bit - jobs shouldn't process while paused
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&processed))

	// Resume the queue
	err = queue.Resume()
	assert.NoError(t, err)
	assert.False(t, queue.IsPaused())

	// Wait for processing
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(2), atomic.LoadInt32(&processed))

	queue.StopConsuming()
}

func TestQueue_GetCounts(t *testing.T) {
	queue := setupTestQueue(t)

	// Initially empty
	waiting, _ := queue.GetWaitingCount()
	active, _ := queue.GetActiveCount()
	failed, _ := queue.GetFailedCount()

	assert.Equal(t, 0, waiting)
	assert.Equal(t, 0, active)
	assert.Equal(t, 0, failed)

	// Add jobs
	queue.Add("job1")
	queue.Add("job2")
	queue.Add("job3")

	waiting, _ = queue.GetWaitingCount()
	assert.Equal(t, 3, waiting)

	// Process with a slow handler to keep jobs active
	handler := func(job Job) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}

	queue.Process(handler, 2)
	time.Sleep(50 * time.Millisecond) // Let some jobs start processing

	active, _ = queue.GetActiveCount()
	assert.Greater(t, active, 0)

	queue.StopConsuming()
}

func TestQueue_RemoveJob(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("job to remove")

	waiting, _ := queue.GetWaitingCount()
	assert.Equal(t, 1, waiting)

	err := queue.RemoveJob(job.GetID())
	assert.NoError(t, err)

	waiting, _ = queue.GetWaitingCount()
	assert.Equal(t, 0, waiting)
}

func TestQueue_Empty(t *testing.T) {
	queue := setupTestQueue(t)

	// Add multiple jobs
	queue.Add("job1")
	queue.Add("job2")
	queue.Add("job3")

	waiting, _ := queue.GetWaitingCount()
	assert.Equal(t, 3, waiting)

	// Empty the queue
	err := queue.Empty()
	assert.NoError(t, err)

	waiting, _ = queue.GetWaitingCount()
	active, _ := queue.GetActiveCount()
	failed, _ := queue.GetFailedCount()

	assert.Equal(t, 0, waiting)
	assert.Equal(t, 0, active)
	assert.Equal(t, 0, failed)
}

func TestQueue_BulkRemove(t *testing.T) {
	queue := setupTestQueue(t)

	job1, _ := queue.Add("job1")
	job2, _ := queue.Add("job2")
	job3, _ := queue.Add("job3")

	jobIDs := []string{job1.GetID(), job2.GetID()}

	err := queue.BulkRemove(jobIDs)
	assert.NoError(t, err)

	waiting, _ := queue.GetWaitingCount()
	assert.Equal(t, 1, waiting)

	// job3 should still exist
	_, err = queue.GetJob(job3.GetID())
	assert.NoError(t, err)
}

func TestQueue_BulkPause(t *testing.T) {
	queue := setupTestQueue(t)

	job1, _ := queue.Add("job1")
	job2, _ := queue.Add("job2")

	jobIDs := []string{job1.GetID(), job2.GetID()}

	err := queue.BulkPause(jobIDs)
	assert.NoError(t, err)

	job1.Refresh()
	job2.Refresh()

	assert.Equal(t, StatusPaused, job1.GetStatus())
	assert.Equal(t, StatusPaused, job2.GetStatus())
}

func TestQueue_BulkResume(t *testing.T) {
	queue := setupTestQueue(t)

	job1, _ := queue.Add("job1")
	job2, _ := queue.Add("job2")

	jobIDs := []string{job1.GetID(), job2.GetID()}

	// Pause jobs first
	queue.BulkPause(jobIDs)

	job1.Refresh()
	assert.Equal(t, StatusPaused, job1.GetStatus())

	// Resume jobs
	err := queue.BulkResume(jobIDs)
	assert.NoError(t, err)

	job1.Refresh()
	job2.Refresh()

	assert.Equal(t, StatusWaiting, job1.GetStatus())
	assert.Equal(t, StatusWaiting, job2.GetStatus())
}

func TestQueue_StopConsuming(t *testing.T) {
	queue := setupTestQueue(t)

	queue.Add("job1")
	queue.Add("job2")

	handler := func(job Job) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	queue.Process(handler, 2)

	// Let processing start
	time.Sleep(50 * time.Millisecond)

	// Stop consuming
	err := queue.StopConsuming()
	assert.NoError(t, err)

	// Should be idempotent
	err = queue.StopConsuming()
	assert.NoError(t, err)
}

func TestQueue_ConcurrentProducers(t *testing.T) {
	queue := setupTestQueue(t)

	var wg sync.WaitGroup
	jobCount := 50

	// Add jobs concurrently
	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			queue.Add(fmt.Sprintf("job-%d", id))
		}(i)
	}

	wg.Wait()

	waiting, err := queue.GetWaitingCount()
	require.NoError(t, err)
	assert.Equal(t, jobCount, waiting)
}

func TestQueue_ConcurrentConsumers(t *testing.T) {
	queue := setupTestQueue(t)

	jobCount := 20
	for i := 0; i < jobCount; i++ {
		queue.Add(fmt.Sprintf("job-%d", i))
	}

	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	}

	// Process with multiple workers
	err := queue.Process(handler, 5)
	require.NoError(t, err)

	// Wait for all jobs to process
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for jobs to process")
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

func TestQueue_GetJob(t *testing.T) {
	queue := setupTestQueue(t)

	originalJob, _ := queue.Add("test data")
	jobID := originalJob.GetID()

	// Get the job
	retrievedJob, err := queue.GetJob(jobID)
	require.NoError(t, err)

	assert.Equal(t, jobID, retrievedJob.GetID())
	assert.Equal(t, "test data", retrievedJob.GetData())
	assert.Equal(t, StatusWaiting, retrievedJob.GetStatus())
}

func TestQueue_GetJob_NotFound(t *testing.T) {
	queue := setupTestQueue(t)

	_, err := queue.GetJob("non-existent-job-id")
	assert.Error(t, err)
}

func TestQueue_ProcessWithInvalidConcurrency(t *testing.T) {
	queue := setupTestQueue(t)

	handler := func(job Job) error {
		return nil
	}

	err := queue.Process(handler, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "concurrency must be at least 1")

	err = queue.Process(handler, -1)
	assert.Error(t, err)
}

func TestQueue_RateLimiter(t *testing.T) {
	queue := setupTestQueue(t)

	// Set rate limit: 2 jobs per second
	err := queue.SetRateLimit(2, 1*time.Second)
	assert.NoError(t, err)

	// Add 5 jobs
	for i := 0; i < 5; i++ {
		queue.Add(fmt.Sprintf("job-%d", i))
	}

	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	queue.Process(handler, 1)

	// After 1 second, should have processed ~2 jobs
	time.Sleep(1100 * time.Millisecond)
	count := atomic.LoadInt32(&processed)
	assert.LessOrEqual(t, count, int32(4)) // Allow margin for timing/scheduling variations

	// After 2 more seconds, should process more
	time.Sleep(2 * time.Second)
	count = atomic.LoadInt32(&processed)
	assert.GreaterOrEqual(t, count, int32(4))

	queue.StopConsuming()
}

func TestQueue_ProgressTracking(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("progress job")

	handler := func(j Job) error {
		j.UpdateProgress(25)
		time.Sleep(50 * time.Millisecond)
		j.UpdateProgress(50)
		time.Sleep(50 * time.Millisecond)
		j.UpdateProgress(75)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	queue.Process(handler, 1)

	// Wait for processing to complete
	time.Sleep(500 * time.Millisecond)

	job.Refresh()
	assert.Equal(t, StatusCompleted, job.GetStatus())

	queue.StopConsuming()
}

func TestQueue_MaxRetriesReached(t *testing.T) {
	queue := setupTestQueue(t)

	opts := JobOptions{
		Attempts: 2,
	}
	job, _ := queue.AddWithOptions("failing job", opts)

	attempts := int32(0)
	handler := func(j Job) error {
		atomic.AddInt32(&attempts, 1)
		return fmt.Errorf("persistent error")
	}

	queue.Process(handler, 1)

	// Wait for all retries
	time.Sleep(1 * time.Second)

	job.Refresh()
	assert.Equal(t, StatusFailed, job.GetStatus())

	// Should have attempted max times
	assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))

	queue.StopConsuming()
}
