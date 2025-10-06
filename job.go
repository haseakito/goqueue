package goqueue

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Job represents a job in the queue system
type Job interface {
	// GetID returns the unique job identifier
	GetID() string

	// GetData returns the job payload
	GetData() string

	// GetStatus returns the current job status
	GetStatus() JobStatus

	// Refresh updates the job state from Redis
	Refresh() error

	// UpdateProgress updates the progress of the job (0-100)
	UpdateProgress(progress int) error

	// MoveToCompleted marks the job as completed
	MoveToCompleted(result string) error

	// MoveToFailed marks the job as failed
	MoveToFailed(reason string) error

	// Retry retries the job (moves it back to ready queue)
	Retry() error

	// Remove removes the job from the queue
	Remove() error

	// GetAttempts returns the number of times this job has been attempted
	GetAttempts() int

	// GetTimestamp returns when the job was created
	GetTimestamp() time.Time
}

// JobOptions contains optional parameters for job creation
type JobOptions struct {
	Priority  int           // Higher priority jobs are processed first
	Delay     time.Duration // Delay before the job becomes ready
	Attempts  int           // Maximum number of retry attempts
	Timeout   time.Duration // Job timeout duration
	Timestamp time.Time     // Job creation timestamp
}

// DefaultJobOptions returns default job options
func DefaultJobOptions() JobOptions {
	return JobOptions{
		Priority:  0,
		Delay:     0,
		Attempts:  3,
		Timeout:   30 * time.Second,
		Timestamp: time.Now(),
	}
}

// redisJob implements the Job interface
type redisJob struct {
	ID          string    `json:"id"`
	Data        string    `json:"data"`
	Status      JobStatus `json:"status"`
	Progress    int       `json:"progress"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	Priority    int       `json:"priority"`
	Timestamp   time.Time `json:"timestamp"`
	Result      string    `json:"result,omitempty"`
	Error       string    `json:"error,omitempty"`

	queue       *redisQueue
	redisClient RedisClient
	mu          sync.RWMutex
}

// GetID returns the job ID
func (j *redisJob) GetID() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.ID
}

// GetData returns the job data payload
func (j *redisJob) GetData() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Data
}

// GetStatus returns the current job status
func (j *redisJob) GetStatus() JobStatus {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Status
}

// GetAttempts returns the number of attempts
func (j *redisJob) GetAttempts() int {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Attempts
}

// GetTimestamp returns the job creation timestamp
func (j *redisJob) GetTimestamp() time.Time {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Timestamp
}

// Refresh reloads the job data from Redis
func (j *redisJob) Refresh() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	jobKey := j.getJobKey()
	data, err := j.redisClient.HGetAll(jobKey)
	if err != nil {
		return fmt.Errorf("failed to refresh job: %w", err)
	}

	if len(data) == 0 {
		return ErrNotFound
	}

	// Parse the job data
	if dataStr, ok := data["data"]; ok {
		j.Data = dataStr
	}
	if statusStr, ok := data["status"]; ok {
		j.Status = JobStatus(statusStr)
	}
	if progressStr, ok := data["progress"]; ok {
		fmt.Sscanf(progressStr, "%d", &j.Progress)
	}
	if attemptsStr, ok := data["attempts"]; ok {
		fmt.Sscanf(attemptsStr, "%d", &j.Attempts)
	}
	if maxAttemptsStr, ok := data["max_attempts"]; ok {
		fmt.Sscanf(maxAttemptsStr, "%d", &j.MaxAttempts)
	}
	if resultStr, ok := data["result"]; ok {
		j.Result = resultStr
	}
	if errorStr, ok := data["error"]; ok {
		j.Error = errorStr
	}

	return nil
}

// UpdateProgress updates the job's progress (0-100)
func (j *redisJob) UpdateProgress(progress int) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if progress < 0 || progress > 100 {
		return fmt.Errorf("progress must be between 0 and 100")
	}

	j.Progress = progress
	jobKey := j.getJobKey()
	_, err := j.redisClient.HSet(jobKey, "progress", fmt.Sprintf("%d", progress))
	return err
}

// MoveToCompleted marks the job as completed and stores the result
func (j *redisJob) MoveToCompleted(result string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.Status = StatusCompleted
	j.Result = result
	j.Progress = 100

	jobKey := j.getJobKey()
	pipe := j.redisClient.Pipeline()
	pipe.HSet(jobKey, "status", string(StatusCompleted))
	pipe.HSet(jobKey, "result", result)
	pipe.HSet(jobKey, "progress", "100")
	pipe.HSet(jobKey, "completed_at", time.Now().Format(time.RFC3339))

	if err := pipe.Exec(); err != nil {
		return fmt.Errorf("failed to move job to completed: %w", err)
	}

	// Remove from unacked list if present
	if j.queue != nil {
		unackedKey := j.queue.getUnackedKey()
		_, _ = j.redisClient.LRem(unackedKey, 0, j.ID)
	}

	return nil
}

// MoveToFailed marks the job as failed and stores the error
func (j *redisJob) MoveToFailed(reason string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.Status = StatusFailed
	j.Error = reason

	jobKey := j.getJobKey()
	pipe := j.redisClient.Pipeline()
	pipe.HSet(jobKey, "status", string(StatusFailed))
	pipe.HSet(jobKey, "error", reason)
	pipe.HSet(jobKey, "failed_at", time.Now().Format(time.RFC3339))

	if err := pipe.Exec(); err != nil {
		return fmt.Errorf("failed to move job to failed: %w", err)
	}

	// Move to rejected queue if we have a queue reference
	if j.queue != nil {
		rejectedKey := j.queue.getRejectedKey()
		if _, err := j.redisClient.RPush(rejectedKey, j.ID); err != nil {
			return fmt.Errorf("failed to add job to rejected queue: %w", err)
		}

		// Remove from unacked list
		unackedKey := j.queue.getUnackedKey()
		_, _ = j.redisClient.LRem(unackedKey, 0, j.ID)
	}

	return nil
}

// Retry moves the job back to the ready queue for retry
func (j *redisJob) Retry() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.Attempts >= j.MaxAttempts {
		return fmt.Errorf("job has reached maximum retry attempts (%d)", j.MaxAttempts)
	}

	j.Attempts++
	j.Status = StatusWaiting

	jobKey := j.getJobKey()
	pipe := j.redisClient.Pipeline()
	pipe.HSet(jobKey, "status", string(StatusWaiting))
	pipe.HSet(jobKey, "attempts", fmt.Sprintf("%d", j.Attempts))

	if err := pipe.Exec(); err != nil {
		return fmt.Errorf("failed to update job for retry: %w", err)
	}

	// Move back to ready queue if we have a queue reference
	if j.queue != nil {
		readyKey := j.queue.getReadyKey()
		if _, err := j.redisClient.LPush(readyKey, j.ID); err != nil {
			return fmt.Errorf("failed to add job to ready queue: %w", err)
		}

		// Remove from unacked or rejected list
		unackedKey := j.queue.getUnackedKey()
		_, _ = j.redisClient.LRem(unackedKey, 0, j.ID)

		rejectedKey := j.queue.getRejectedKey()
		_, _ = j.redisClient.LRem(rejectedKey, 0, j.ID)
	}

	return nil
}

// Remove removes the job from the queue and deletes its data
func (j *redisJob) Remove() error {
	jobKey := j.getJobKey()

	// Delete the job data
	if _, err := j.redisClient.Del(jobKey); err != nil {
		return fmt.Errorf("failed to delete job data: %w", err)
	}

	// Remove from all lists if we have a queue reference
	if j.queue != nil {
		readyKey := j.queue.getReadyKey()
		_, _ = j.redisClient.LRem(readyKey, 0, j.ID)

		unackedKey := j.queue.getUnackedKey()
		_, _ = j.redisClient.LRem(unackedKey, 0, j.ID)

		rejectedKey := j.queue.getRejectedKey()
		_, _ = j.redisClient.LRem(rejectedKey, 0, j.ID)
	}

	return nil
}

// getJobKey returns the Redis key for this job's data
func (j *redisJob) getJobKey() string {
	if j.queue != nil {
		return fmt.Sprintf("goqueue::queue::%s::job::%s", j.queue.name, j.ID)
	}
	return fmt.Sprintf("goqueue::job::%s", j.ID)
}

// MarshalBinary implements encoding.BinaryMarshaler
func (j *redisJob) MarshalBinary() ([]byte, error) {
	return json.Marshal(j)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (j *redisJob) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, j)
}
