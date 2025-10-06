package goqueue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter_NotExceeded(t *testing.T) {
	rc := setupTestRedis(t)

	limiter := newRateLimiter(rc, "test-queue", 5, 1*time.Second)

	// First request should not exceed
	exceeded, wait, err := limiter.isExceeded()
	require.NoError(t, err)
	assert.False(t, exceeded)
	assert.Equal(t, time.Duration(0), wait)
}

func TestRateLimiter_Exceeded(t *testing.T) {
	rc := setupTestRedis(t)

	// Set limit to 3 requests per second
	limiter := newRateLimiter(rc, "test-queue", 3, 1*time.Second)

	// Make 3 requests (should all succeed)
	for i := 0; i < 3; i++ {
		exceeded, _, err := limiter.isExceeded()
		require.NoError(t, err)
		assert.False(t, exceeded, "Request %d should not exceed limit", i+1)
	}

	// 4th request should exceed
	exceeded, wait, err := limiter.isExceeded()
	require.NoError(t, err)
	assert.True(t, exceeded)
	assert.Greater(t, wait, time.Duration(0))
	assert.LessOrEqual(t, wait, 1*time.Second)
}

func TestRateLimiter_ResetAfterDuration(t *testing.T) {
	rc := setupTestRedis(t)

	// Set limit to 2 requests per 500ms
	limiter := newRateLimiter(rc, "test-queue", 2, 500*time.Millisecond)

	// Use up the limit
	limiter.isExceeded()
	limiter.isExceeded()

	// Should be exceeded
	exceeded, _, err := limiter.isExceeded()
	require.NoError(t, err)
	assert.True(t, exceeded)

	// Wait for the duration to pass
	time.Sleep(600 * time.Millisecond)

	// Should not be exceeded anymore
	exceeded, _, err = limiter.isExceeded()
	require.NoError(t, err)
	assert.False(t, exceeded)
}

func TestRateLimiter_MultipleQueues(t *testing.T) {
	rc := setupTestRedis(t)

	limiter1 := newRateLimiter(rc, "queue-1", 2, 1*time.Second)
	limiter2 := newRateLimiter(rc, "queue-2", 2, 1*time.Second)

	// Use up queue-1 limit
	limiter1.isExceeded()
	limiter1.isExceeded()

	// queue-1 should be exceeded
	exceeded, _, err := limiter1.isExceeded()
	require.NoError(t, err)
	assert.True(t, exceeded)

	// queue-2 should not be affected
	exceeded, _, err = limiter2.isExceeded()
	require.NoError(t, err)
	assert.False(t, exceeded)
}

func TestRateLimiter_ConcurrentRequests(t *testing.T) {
	rc := setupTestRedis(t)

	limiter := newRateLimiter(rc, "concurrent-queue", 10, 1*time.Second)

	var wg sync.WaitGroup
	var exceededCount int32

	// Make 20 concurrent requests
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			exceeded, _, err := limiter.isExceeded()
			require.NoError(t, err)
			if exceeded {
				atomic.AddInt32(&exceededCount, 1)
			}
		}()
	}

	wg.Wait()

	// At least some requests should have been rate limited
	assert.Greater(t, atomic.LoadInt32(&exceededCount), int32(0))
}

func TestRateLimiter_DifferentDurations(t *testing.T) {
	rc := setupTestRedis(t)

	// Short duration
	limiter := newRateLimiter(rc, "test-queue", 3, 200*time.Millisecond)

	// Use up the limit
	for i := 0; i < 3; i++ {
		exceeded, _, _ := limiter.isExceeded()
		assert.False(t, exceeded)
	}

	// Should be exceeded
	exceeded, _, err := limiter.isExceeded()
	require.NoError(t, err)
	assert.True(t, exceeded)

	// Wait for short duration
	time.Sleep(250 * time.Millisecond)

	// Should be available again
	exceeded, _, err = limiter.isExceeded()
	require.NoError(t, err)
	assert.False(t, exceeded)
}

func TestRateLimiter_ZeroLimit(t *testing.T) {
	rc := setupTestRedis(t)

	limiter := newRateLimiter(rc, "test-queue", 0, 1*time.Second)

	// Should immediately be exceeded with 0 limit
	exceeded, _, err := limiter.isExceeded()
	require.NoError(t, err)
	assert.True(t, exceeded)
}

func TestRateLimiter_HighLimit(t *testing.T) {
	rc := setupTestRedis(t)

	limiter := newRateLimiter(rc, "test-queue", 100, 1*time.Second)

	// Make many requests
	for i := 0; i < 50; i++ {
		exceeded, _, err := limiter.isExceeded()
		require.NoError(t, err)
		assert.False(t, exceeded)
	}
}

func TestRateLimiter_WaitDuration(t *testing.T) {
	rc := setupTestRedis(t)

	limiter := newRateLimiter(rc, "test-queue", 1, 1*time.Second)

	// First request
	limiter.isExceeded()

	// Second request should exceed
	exceeded, wait, err := limiter.isExceeded()
	require.NoError(t, err)
	assert.True(t, exceeded)
	assert.Greater(t, wait, time.Duration(0))
	assert.LessOrEqual(t, wait, 1*time.Second)

	// Wait should decrease over time
	time.Sleep(200 * time.Millisecond)
	exceeded, newWait, err := limiter.isExceeded()
	require.NoError(t, err)
	assert.True(t, exceeded)
	assert.Less(t, newWait, wait)
}
