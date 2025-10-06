package goqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestQueue(t *testing.T) Queue {
	conn, _ := setupTestConnection(t)
	queue, err := conn.OpenQueue("test-queue")
	require.NoError(t, err)
	return queue
}

func TestJob_Add(t *testing.T) {
	queue := setupTestQueue(t)

	job, err := queue.Add("test job data")
	require.NoError(t, err)
	require.NotNil(t, job)

	assert.NotEmpty(t, job.GetID())
	assert.Equal(t, "test job data", job.GetData())
	assert.Equal(t, StatusWaiting, job.GetStatus())
	assert.Equal(t, 0, job.GetAttempts())
}

func TestJob_AddWithOptions(t *testing.T) {
	queue := setupTestQueue(t)

	opts := JobOptions{
		Priority:  5,
		Attempts:  10,
		Timeout:   60 * time.Second,
		Timestamp: time.Now(),
	}

	job, err := queue.AddWithOptions("high priority job", opts)
	require.NoError(t, err)

	assert.NotEmpty(t, job.GetID())
	assert.Equal(t, "high priority job", job.GetData())

	// Check that the job was created with correct options
	if rj, ok := job.(*redisJob); ok {
		assert.Equal(t, 5, rj.Priority)
		assert.Equal(t, 10, rj.MaxAttempts)
	}
}

func TestJob_UpdateProgress(t *testing.T) {
	queue := setupTestQueue(t)

	job, err := queue.Add("test job")
	require.NoError(t, err)

	// Update progress
	err = job.UpdateProgress(50)
	assert.NoError(t, err)

	// Refresh and check
	err = job.Refresh()
	require.NoError(t, err)

	if rj, ok := job.(*redisJob); ok {
		assert.Equal(t, 50, rj.Progress)
	}
}

func TestJob_UpdateProgress_Invalid(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("test job")

	// Test invalid progress values
	err := job.UpdateProgress(-1)
	assert.Error(t, err)

	err = job.UpdateProgress(101)
	assert.Error(t, err)
}

func TestJob_MoveToCompleted(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("test job")

	err := job.MoveToCompleted("success result")
	assert.NoError(t, err)

	// Refresh and check
	err = job.Refresh()
	require.NoError(t, err)

	assert.Equal(t, StatusCompleted, job.GetStatus())
	if rj, ok := job.(*redisJob); ok {
		assert.Equal(t, "success result", rj.Result)
		assert.Equal(t, 100, rj.Progress)
	}
}

func TestJob_MoveToFailed(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("test job")

	err := job.MoveToFailed("error occurred")
	assert.NoError(t, err)

	// Refresh and check
	err = job.Refresh()
	require.NoError(t, err)

	assert.Equal(t, StatusFailed, job.GetStatus())
	if rj, ok := job.(*redisJob); ok {
		assert.Equal(t, "error occurred", rj.Error)
	}

	// Job should be in rejected queue
	failedCount, _ := queue.GetFailedCount()
	assert.Equal(t, 1, failedCount)
}

func TestJob_Retry(t *testing.T) {
	queue := setupTestQueue(t)

	opts := JobOptions{
		Attempts: 3,
	}
	job, _ := queue.AddWithOptions("retry job", opts)

	// Remove from ready queue to simulate processing
	readyKey := queue.(*redisQueue).getReadyKey()
	unackedKey := queue.(*redisQueue).getUnackedKey()
	redisClient := queue.(*redisQueue).connection.redisClient
	redisClient.RPopLPush(readyKey, unackedKey)

	// First retry
	err := job.Retry()
	assert.NoError(t, err)
	assert.Equal(t, 1, job.GetAttempts())
	assert.Equal(t, StatusWaiting, job.GetStatus())

	// Job should be back in ready queue
	waitingCount, _ := queue.GetWaitingCount()
	assert.Equal(t, 1, waitingCount)
}

func TestJob_Retry_MaxAttemptsReached(t *testing.T) {
	queue := setupTestQueue(t)

	opts := JobOptions{
		Attempts: 2,
	}
	job, _ := queue.AddWithOptions("retry job", opts)

	// Set attempts to max
	if rj, ok := job.(*redisJob); ok {
		rj.Attempts = 2
	}

	err := job.Retry()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maximum retry attempts")
}

func TestJob_Remove(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("job to remove")
	jobID := job.GetID()

	// Verify job exists
	waitingCount, _ := queue.GetWaitingCount()
	assert.Equal(t, 1, waitingCount)

	// Remove job
	err := job.Remove()
	assert.NoError(t, err)

	// Verify job is gone
	waitingCount, _ = queue.GetWaitingCount()
	assert.Equal(t, 0, waitingCount)

	// Try to get the job
	_, err = queue.GetJob(jobID)
	assert.Error(t, err)
}

func TestJob_Refresh(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("test job")

	// Update the job
	job.UpdateProgress(75)

	// Create a new job instance with the same ID
	newJob := &redisJob{
		ID:          job.GetID(),
		queue:       queue.(*redisQueue),
		redisClient: queue.(*redisQueue).connection.redisClient,
	}

	// Refresh should load the data
	err := newJob.Refresh()
	require.NoError(t, err)

	assert.Equal(t, job.GetData(), newJob.GetData())
	assert.Equal(t, 75, newJob.Progress)
}

func TestJob_Refresh_NotFound(t *testing.T) {
	queue := setupTestQueue(t)

	job := &redisJob{
		ID:          "non-existent-job",
		queue:       queue.(*redisQueue),
		redisClient: queue.(*redisQueue).connection.redisClient,
	}

	err := job.Refresh()
	assert.Equal(t, ErrNotFound, err)
}

func TestJob_GetTimestamp(t *testing.T) {
	queue := setupTestQueue(t)

	beforeTime := time.Now()
	job, _ := queue.Add("test job")
	afterTime := time.Now()

	timestamp := job.GetTimestamp()
	assert.True(t, timestamp.After(beforeTime) || timestamp.Equal(beforeTime))
	assert.True(t, timestamp.Before(afterTime) || timestamp.Equal(afterTime))
}

func TestJobOptions_Default(t *testing.T) {
	opts := DefaultJobOptions()

	assert.Equal(t, 0, opts.Priority)
	assert.Equal(t, time.Duration(0), opts.Delay)
	assert.Equal(t, 3, opts.Attempts)
	assert.Equal(t, 30*time.Second, opts.Timeout)
	assert.NotZero(t, opts.Timestamp)
}

func TestJob_MarshalUnmarshal(t *testing.T) {
	job := &redisJob{
		ID:          "test-id",
		Data:        "test data",
		Status:      StatusWaiting,
		Progress:    50,
		Attempts:    1,
		MaxAttempts: 3,
		Priority:    5,
		Timestamp:   time.Now(),
	}

	// Marshal
	data, err := job.MarshalBinary()
	require.NoError(t, err)

	// Unmarshal
	newJob := &redisJob{}
	err = newJob.UnmarshalBinary(data)
	require.NoError(t, err)

	assert.Equal(t, job.ID, newJob.ID)
	assert.Equal(t, job.Data, newJob.Data)
	assert.Equal(t, job.Status, newJob.Status)
	assert.Equal(t, job.Progress, newJob.Progress)
	assert.Equal(t, job.Attempts, newJob.Attempts)
	assert.Equal(t, job.Priority, newJob.Priority)
}

func TestJob_ConcurrentUpdates(t *testing.T) {
	queue := setupTestQueue(t)

	job, _ := queue.Add("concurrent test")

	// Update progress concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(progress int) {
			job.UpdateProgress(progress * 10)
			done <- true
		}(i)
	}

	// Wait for all updates
	for i := 0; i < 10; i++ {
		<-done
	}

	// Refresh and verify
	err := job.Refresh()
	assert.NoError(t, err)
}

func TestJob_DelayedExecution(t *testing.T) {
	queue := setupTestQueue(t)

	opts := JobOptions{
		Delay:    200 * time.Millisecond,
		Attempts: 3,
	}

	beforeCount, _ := queue.GetWaitingCount()

	job, err := queue.AddWithOptions("delayed job", opts)
	require.NoError(t, err)
	assert.NotNil(t, job)

	// Job should not be immediately available
	immediateCount, _ := queue.GetWaitingCount()
	assert.Equal(t, beforeCount, immediateCount)

	// Wait for delay to pass
	time.Sleep(300 * time.Millisecond)

	// Job should now be available
	afterCount, _ := queue.GetWaitingCount()
	assert.Equal(t, beforeCount+1, afterCount)
}
