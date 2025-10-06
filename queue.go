package goqueue

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Queue represents a job queue
type Queue interface {
	// Add adds a new job to the queue
	Add(data string) (Job, error)

	// AddWithOptions adds a new job with custom options
	AddWithOptions(data string, options JobOptions) (Job, error)

	// Process starts consuming jobs from the queue
	Process(handler JobHandler, concurrency int) error

	// GetJob retrieves a job by ID
	GetJob(jobID string) (Job, error)

	// Pause pauses the queue (stops processing new jobs)
	Pause() error

	// Resume resumes the queue
	Resume() error

	// IsPaused returns whether the queue is paused
	IsPaused() bool

	// SetRateLimit sets a rate limit for the queue
	SetRateLimit(limit int, duration time.Duration) error

	// RemoveJob removes a specific job by ID
	RemoveJob(jobID string) error

	// Empty removes all jobs from the queue
	Empty() error

	// BulkRemove removes multiple jobs by ID
	BulkRemove(jobIDs []string) error

	// BulkPause pauses multiple jobs
	BulkPause(jobIDs []string) error

	// BulkResume resumes multiple jobs
	BulkResume(jobIDs []string) error

	// GetWaitingCount returns the number of jobs waiting in the ready queue
	GetWaitingCount() (int, error)

	// GetActiveCount returns the number of jobs currently being processed
	GetActiveCount() (int, error)

	// GetFailedCount returns the number of failed jobs
	GetFailedCount() (int, error)

	// StopConsuming stops consuming jobs
	StopConsuming() error

	// GetName returns the queue name
	GetName() string
}

// JobHandler is a function that processes a job
type JobHandler func(job Job) error

// redisQueue implements the Queue interface
type redisQueue struct {
	name        string
	connection  *redisConnection
	paused      bool
	pauseMutex  sync.RWMutex
	rateLimiter *RateLimiter

	// Consumer state
	consumingStopped bool
	stopChannel      chan struct{}
	consumerWg       sync.WaitGroup
}

// newQueue creates a new queue instance
func newQueue(name string, connection *redisConnection) Queue {
	return &redisQueue{
		name:             name,
		connection:       connection,
		paused:           false,
		consumingStopped: true,
		stopChannel:      make(chan struct{}),
	}
}

// SetRateLimit sets a rate limit for the queue
func (q *redisQueue) SetRateLimit(limit int, duration time.Duration) error {
	q.rateLimiter = newRateLimiter(q.connection.redisClient, q.name, limit, duration)
	return nil
}

// GetName returns the queue name
func (q *redisQueue) GetName() string {
	return q.name
}

// Add adds a new job to the queue with default options
func (q *redisQueue) Add(data string) (Job, error) {
	return q.AddWithOptions(data, DefaultJobOptions())
}

// AddWithOptions adds a new job to the queue with custom options
func (q *redisQueue) AddWithOptions(data string, options JobOptions) (Job, error) {
	jobID := fmt.Sprintf("%s-%d", RandomString(16), time.Now().UnixNano())

	job := &redisJob{
		ID:          jobID,
		Data:        data,
		Status:      StatusWaiting,
		Progress:    0,
		Attempts:    0,
		MaxAttempts: options.Attempts,
		Priority:    options.Priority,
		Timestamp:   options.Timestamp,
		queue:       q,
		redisClient: q.connection.redisClient,
	}

	// Store job data in Redis hash
	jobKey := job.getJobKey()
	pipe := q.connection.redisClient.Pipeline()
	pipe.HSet(jobKey, "id", job.ID)
	pipe.HSet(jobKey, "data", job.Data)
	pipe.HSet(jobKey, "status", string(job.Status))
	pipe.HSet(jobKey, "progress", "0")
	pipe.HSet(jobKey, "attempts", "0")
	pipe.HSet(jobKey, "max_attempts", fmt.Sprintf("%d", job.MaxAttempts))
	pipe.HSet(jobKey, "priority", fmt.Sprintf("%d", job.Priority))
	pipe.HSet(jobKey, "created_at", job.Timestamp.Format(time.RFC3339))

	if err := pipe.Exec(); err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	// Add to ready queue (with delay support)
	if options.Delay > 0 {
		// For delayed jobs, we'll add them to ready queue after the delay
		// In a production system, you'd want to use a sorted set with scores as timestamps
		go func() {
			time.Sleep(options.Delay)
			readyKey := q.getReadyKey()
			q.connection.redisClient.LPush(readyKey, jobID)
		}()
	} else {
		readyKey := q.getReadyKey()
		if _, err := q.connection.redisClient.LPush(readyKey, jobID); err != nil {
			return nil, fmt.Errorf("failed to add job to ready queue: %w", err)
		}
	}

	return job, nil
}

// Process starts consuming jobs from the queue with specified concurrency
func (q *redisQueue) Process(handler JobHandler, concurrency int) error {
	if concurrency < 1 {
		return fmt.Errorf("concurrency must be at least 1")
	}

	q.consumingStopped = false
	q.stopChannel = make(chan struct{})

	// Start multiple consumer goroutines
	for i := 0; i < concurrency; i++ {
		q.consumerWg.Add(1)
		go q.consume(handler)
	}

	return nil
}

// consume is the main consumer loop
func (q *redisQueue) consume(handler JobHandler) {
	defer q.consumerWg.Done()

	for {
		select {
		case <-q.stopChannel:
			return
		default:
			// Check if paused
			if q.IsPaused() {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Check rate limit
			if q.rateLimiter != nil {
				exceeded, wait, err := q.rateLimiter.isExceeded()
				if err != nil {
					if q.connection.errChan != nil {
						q.connection.errChan <- fmt.Errorf("failed to check rate limit: %w", err)
					}
					time.Sleep(time.Second)
					continue
				}
				if exceeded {
					time.Sleep(wait)
					continue
				}
			}

			// Pop job from ready queue and push to unacked
			readyKey := q.getReadyKey()
			unackedKey := q.getUnackedKey()

			jobID, err := q.connection.redisClient.RPopLPush(readyKey, unackedKey)
			if err == ErrNotFound || jobID == "" {
				// No jobs available, wait a bit
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				if q.connection.errChan != nil {
					q.connection.errChan <- fmt.Errorf("failed to pop job: %w", err)
				}
				continue
			}

			// Process the job
			job, err := q.GetJob(jobID)
			if err != nil {
				if q.connection.errChan != nil {
					q.connection.errChan <- fmt.Errorf("failed to get job %s: %w", jobID, err)
				}
				continue
			}

			// Update job status to active
			if rj, ok := job.(*redisJob); ok {
				rj.Status = StatusActive
				jobKey := rj.getJobKey()
				q.connection.redisClient.HSet(jobKey, "status", string(StatusActive))
				q.connection.redisClient.HSet(jobKey, "started_at", time.Now().Format(time.RFC3339))
			}

			// Execute the handler
			err = handler(job)

			// Handle the result
			if err != nil {
				// Job failed
				if rj, ok := job.(*redisJob); ok {
					if rj.Attempts < rj.MaxAttempts-1 {
						// Retry the job
						if retryErr := job.Retry(); retryErr != nil && q.connection.errChan != nil {
							q.connection.errChan <- fmt.Errorf("failed to retry job %s: %w", jobID, retryErr)
						}
					} else {
						// Max retries reached, move to failed
						if failErr := job.MoveToFailed(err.Error()); failErr != nil && q.connection.errChan != nil {
							q.connection.errChan <- fmt.Errorf("failed to move job %s to failed: %w", jobID, failErr)
						}
					}
				}
			} else {
				// Job succeeded
				if completeErr := job.MoveToCompleted("success"); completeErr != nil && q.connection.errChan != nil {
					q.connection.errChan <- fmt.Errorf("failed to complete job %s: %w", jobID, completeErr)
				}
			}
		}
	}
}

// GetJob retrieves a job by its ID
func (q *redisQueue) GetJob(jobID string) (Job, error) {
	job := &redisJob{
		ID:          jobID,
		queue:       q,
		redisClient: q.connection.redisClient,
	}

	if err := job.Refresh(); err != nil {
		return nil, err
	}

	return job, nil
}

// Pause pauses the queue
func (q *redisQueue) Pause() error {
	q.pauseMutex.Lock()
	defer q.pauseMutex.Unlock()
	q.paused = true
	return nil
}

// Resume resumes the queue
func (q *redisQueue) Resume() error {
	q.pauseMutex.Lock()
	defer q.pauseMutex.Unlock()
	q.paused = false
	return nil
}

// IsPaused returns whether the queue is paused
func (q *redisQueue) IsPaused() bool {
	q.pauseMutex.RLock()
	defer q.pauseMutex.RUnlock()
	return q.paused
}

// RemoveJob removes a specific job by ID
func (q *redisQueue) RemoveJob(jobID string) error {
	job, err := q.GetJob(jobID)
	if err != nil {
		return err
	}
	return job.Remove()
}

// Empty removes all jobs from the queue
func (q *redisQueue) Empty() error {
	readyKey := q.getReadyKey()
	unackedKey := q.getUnackedKey()
	rejectedKey := q.getRejectedKey()

	pipe := q.connection.redisClient.Pipeline()
	pipe.Del(readyKey)
	pipe.Del(unackedKey)
	pipe.Del(rejectedKey)

	return pipe.Exec()
}

// BulkRemove removes multiple jobs by ID
func (q *redisQueue) BulkRemove(jobIDs []string) error {
	for _, jobID := range jobIDs {
		if err := q.RemoveJob(jobID); err != nil {
			return fmt.Errorf("failed to remove job %s: %w", jobID, err)
		}
	}
	return nil
}

// BulkPause pauses multiple jobs
func (q *redisQueue) BulkPause(jobIDs []string) error {
	for _, jobID := range jobIDs {
		job, err := q.GetJob(jobID)
		if err != nil {
			return fmt.Errorf("failed to get job %s: %w", jobID, err)
		}

		if rj, ok := job.(*redisJob); ok {
			rj.Status = StatusPaused
			jobKey := rj.getJobKey()
			if _, err := q.connection.redisClient.HSet(jobKey, "status", string(StatusPaused)); err != nil {
				return fmt.Errorf("failed to pause job %s: %w", jobID, err)
			}
		}
	}
	return nil
}

// BulkResume resumes multiple jobs
func (q *redisQueue) BulkResume(jobIDs []string) error {
	for _, jobID := range jobIDs {
		job, err := q.GetJob(jobID)
		if err != nil {
			return fmt.Errorf("failed to get job %s: %w", jobID, err)
		}

		if rj, ok := job.(*redisJob); ok {
			if rj.Status == StatusPaused {
				rj.Status = StatusWaiting
				jobKey := rj.getJobKey()
				if _, err := q.connection.redisClient.HSet(jobKey, "status", string(StatusWaiting)); err != nil {
					return fmt.Errorf("failed to resume job %s: %w", jobID, err)
				}
			}
		}
	}
	return nil
}

// GetWaitingCount returns the number of jobs in the ready queue
func (q *redisQueue) GetWaitingCount() (int, error) {
	readyKey := q.getReadyKey()
	return q.connection.redisClient.LLen(readyKey)
}

// GetActiveCount returns the number of jobs in the unacked queue
func (q *redisQueue) GetActiveCount() (int, error) {
	unackedKey := q.getUnackedKey()
	return q.connection.redisClient.LLen(unackedKey)
}

// GetFailedCount returns the number of jobs in the rejected queue
func (q *redisQueue) GetFailedCount() (int, error) {
	rejectedKey := q.getRejectedKey()
	return q.connection.redisClient.LLen(rejectedKey)
}

// StopConsuming stops all consumers
func (q *redisQueue) StopConsuming() error {
	if q.consumingStopped {
		return nil
	}

	q.consumingStopped = true
	close(q.stopChannel)
	q.consumerWg.Wait()

	return nil
}

// Helper methods to get Redis keys

func (q *redisQueue) getReadyKey() string {
	return strings.Replace(q.connection.readyTemplate, phQueue, q.name, 1)
}

func (q *redisQueue) getUnackedKey() string {
	key := strings.Replace(q.connection.unackedTemplate, phConnection, q.connection.Name, 1)
	return strings.Replace(key, phQueue, q.name, 1)
}

func (q *redisQueue) getRejectedKey() string {
	return strings.Replace(q.connection.rejectedTemplate, phQueue, q.name, 1)
}
