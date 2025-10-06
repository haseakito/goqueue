package goqueue

import (
	"fmt"
	"time"
)

const rateLimitScript = `
--[[
  Function to check if a rate limit has been exceeded.
  It uses a ZSET to store timestamps of events.

  Parameters:
    KEYS[1] - The rate limiter key.
    ARGV[1] - The maximum number of events.
    ARGV[2] - The duration in milliseconds.
    ARGV[3] - The current timestamp in milliseconds.
    ARGV[4] - A unique ID for the event.

  Returns:
    - 1 if the rate limit is exceeded.
    - 0 if the rate limit is not exceeded.
    - The TTL of the key in milliseconds if the rate limit is not exceeded.
]]
local key = KEYS[1]
local max_events = tonumber(ARGV[1])
local duration = tonumber(ARGV[2])
local current_time = tonumber(ARGV[3])
local unique_id = ARGV[4]

-- Remove old events
local min_time = current_time - duration
redis.call('ZREMRANGEBYSCORE', key, 0, min_time)

-- Get the current number of events
local event_count = redis.call('ZCARD', key)

if event_count >= max_events then
  -- Rate limit exceeded
  local ttl = redis.call('PTTL', key)
  if ttl < 0 then
    -- In case the key has no expiry, set one to avoid it living forever
    redis.call('PEXPIRE', key, duration)
    ttl = duration
  end
  return {1, ttl}
else
  -- Add the new event
  redis.call('ZADD', key, current_time, unique_id)
  -- Set/update the expiration of the key
  redis.call('PEXPIRE', key, duration)
  return {0, 0}
end
`

// RateLimiter controls the rate of job processing for a queue.
type RateLimiter struct {
	redisClient RedisClient
	key         string
	limit       int
	duration    time.Duration
}

// newRateLimiter creates a new rate limiter.
func newRateLimiter(redisClient RedisClient, queueName string, limit int, duration time.Duration) *RateLimiter {
	key := fmt.Sprintf("goqueue::queue::%s::ratelimit", queueName)
	return &RateLimiter{
		redisClient: redisClient,
		key:         key,
		limit:       limit,
		duration:    duration,
	}
}

// isExceeded checks if the rate limit has been exceeded.
// It returns true if exceeded, along with the duration to wait.
func (r *RateLimiter) isExceeded() (bool, time.Duration, error) {
	keys := []string{r.key}
	uniqueID := RandomString(16)
	args := []interface{}{
		r.limit,
		r.duration.Milliseconds(),
		time.Now().UnixNano() / int64(time.Millisecond),
		uniqueID,
	}

	result, err := r.redisClient.Eval(rateLimitScript, keys, args...)
	if err != nil {
		return false, 0, fmt.Errorf("failed to execute rate limit script: %w", err)
	}

	resSlice, ok := result.([]interface{})
	if !ok || len(resSlice) != 2 {
		return false, 0, fmt.Errorf("unexpected result from rate limit script: %v", result)
	}

	exceeded, ok1 := resSlice[0].(int64)
	wait, ok2 := resSlice[1].(int64)

	if !ok1 || !ok2 {
		return false, 0, fmt.Errorf("unexpected result types from rate limit script: %v", result)
	}

	if exceeded == 1 {
		return true, time.Duration(wait) * time.Millisecond, nil
	}

	return false, 0, nil
}
