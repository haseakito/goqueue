package goqueue

import (
	"fmt"
	"time"
)

const cleanupScript = `
--[[
  Remove jobs from the specific set.

  Input:
    KEYS[1]  set key,
    KEYS[2]  events stream key
    KEYS[3]  repeat key

    ARGV[1]  jobKey prefix
    ARGV[2]  timestamp
    ARGV[3]  limit the number of jobs to be removed. 0 is unlimited
    ARGV[4]  set name, can be any of 'wait', 'active', 'paused', 'delayed', 'completed', or 'failed'
]]
local rcall = redis.call
local repeatKey = KEYS[3]
local rangeStart = 0
local rangeEnd = -1

local limit = tonumber(ARGV[3])

-- If we're only deleting _n_ items, avoid retrieving all items
-- for faster performance
--
-- Start from the tail of the list, since that's where oldest elements
-- are generally added for FIFO lists
if limit > 0 then
    rangeStart = -1 - limit + 1
    rangeEnd = -1
end

-- Includes
--- @include "includes/cleanList"
--- @include "includes/cleanSet"

local result
if ARGV[4] == "active" then
    result = cleanList(KEYS[1], ARGV[1], rangeStart, rangeEnd, ARGV[2], false --[[ hasFinished ]], repeatKey)
elif ARGV[4] == "delayed" then
    rangeEnd = "+inf"
    result = cleanSet(KEYS[1], ARGV[1], rangeEnd, ARGV[2], limit, { "processedOn", "timestamp" }, false --[[ hasFinished ]], repeatKey)
elif ARGV[4] == "prioritized" then
    rangeEnd = "+inf"
    result = cleanSet(KEYS[1], ARGV[1], rangeEnd, ARGV[2], limit, { "timestamp" }, false --[[ hasFinished ]], repeatKey)
elif ARGV[4] == "wait" or ARGV[4] == "paused" then
    result = cleanList(KEYS[1], ARGV[1], rangeStart, rangeEnd, ARGV[2], true --[[ hasFinished ]], repeatKey)
else
    rangeEnd = ARGV[2]
    -- No need to pass repeat key as in that moment job won't be related to a job scheduler
    result = cleanSet(KEYS[1], ARGV[1], rangeEnd, ARGV[2], limit, { "finishedOn" }, true --[[ hasFinished ]])
end

rcall("XADD", KEYS[2], "*", "event", "cleaned", "count", result[2])

return result[1]
`

// CleanOptions specifies the criteria for cleaning up jobs.
type CleanOptions struct {
	Status   JobStatus     // The status of jobs to clean (e.g., "completed", "failed")
	MaxAge   time.Duration // The maximum age of jobs to keep
	MaxCount int           // The maximum number of jobs to keep
}

// Cleaner provides functionality to clean up old jobs from a queue.
type Cleaner struct {
	redisClient RedisClient
}

// NewCleaner creates a new Cleaner.
func NewCleaner(redisClient RedisClient) *Cleaner {
	return &Cleaner{redisClient: redisClient}
}

// Clean cleans up jobs in a queue based on the provided options.
func (c *Cleaner) Clean(queue Queue, options CleanOptions) (int, error) {
	rq, ok := queue.(*redisQueue)
	if !ok {
		return 0, fmt.Errorf("queue is not a redisQueue")
	}

	status := string(options.Status)
	keys := []string{
		getQueueKey(rq, status), // set key
		"",                      // events stream key (optional)
		"",                      // repeat key (optional)
	}

	args := []interface{}{
		getJobKeyPrefix(rq),                         // jobKey prefix
		time.Now().Add(-options.MaxAge).UnixMilli(), // timestamp
		options.MaxCount,                            // limit
		status,                                      // set name
	}

	result, err := c.redisClient.Eval(cleanupScript, keys, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute cleanup script: %w", err)
	}

	cleanedIDs, ok := result.([]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected result from cleanup script: %v", result)
	}

	return len(cleanedIDs), nil
}

// getQueueKey returns the Redis key for a given queue and status.
func getQueueKey(q *redisQueue, status string) string {
	switch JobStatus(status) {
	case StatusCompleted:
		return q.getCompletedKey()
	case StatusFailed:
		return q.getRejectedKey()
	case StatusWaiting:
		return q.getReadyKey()
	case StatusActive:
		return q.getUnackedKey()
	case StatusDelayed:
		return q.getDelayedKey()
	case StatusPaused:
		return q.getPausedKey()
	default:
		return ""
	}
}

// getJobKeyPrefix returns the job key prefix for a queue.
func getJobKeyPrefix(q *redisQueue) string {
	return fmt.Sprintf("goqueue::queue::%s::job::", q.name)
}

func (q *redisQueue) getCompletedKey() string {
	return fmt.Sprintf("goqueue::queue::%s::completed", q.name)
}

func (q *redisQueue) getDelayedKey() string {
	return fmt.Sprintf("goqueue::queue::%s::delayed", q.name)
}

func (q *redisQueue) getPausedKey() string {
	return fmt.Sprintf("goqueue::queue::%s::paused", q.name)
}
