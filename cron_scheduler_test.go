package goqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupCronTest(t *testing.T, queueName string) (Connection, Queue) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})

	ctx := context.Background()
	client.FlushDB(ctx)

	errChan := make(chan error, 100)

	conn, err := OpenConnectionWithRedisClient("test-cron", client, errChan)
	require.NoError(t, err)

	queue, err := conn.OpenQueue(queueName)
	require.NoError(t, err)

	t.Cleanup(func() {
		queue.StopConsuming()
		conn.StopHeartbeat()
		client.FlushDB(ctx)
		client.Close()
		close(errChan)
	})

	return conn, queue
}

func TestParseCronExpression_Valid(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		shouldErr  bool
	}{
		{"every minute", "* * * * *", false},
		{"specific time", "30 2 * * *", false},
		{"range", "0 9-17 * * *", false},
		{"step", "*/15 * * * *", false},
		{"list", "0,15,30,45 * * * *", false},
		{"complex", "*/5 9-17 * * 1-5", false},
		{"weekday mornings", "0 9 * * 1-5", false},
		{"invalid fields", "* * * *", true},
		{"invalid minute", "60 * * * *", true},
		{"invalid hour", "* 24 * * *", true},
		{"invalid day", "* * 32 * *", true},
		{"invalid month", "* * * 13 *", true},
		{"invalid weekday", "* * * * 7", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseCronExpression(tt.expression)
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseField_Wildcard(t *testing.T) {
	result, err := parseField("*", 0, 59)
	require.NoError(t, err)
	assert.Equal(t, 60, len(result))
	assert.Equal(t, 0, result[0])
	assert.Equal(t, 59, result[59])
}

func TestParseField_Range(t *testing.T) {
	result, err := parseField("10-15", 0, 59)
	require.NoError(t, err)
	assert.Equal(t, []int{10, 11, 12, 13, 14, 15}, result)
}

func TestParseField_Step(t *testing.T) {
	result, err := parseField("*/15", 0, 59)
	require.NoError(t, err)
	assert.Contains(t, result, 0)
	assert.Contains(t, result, 15)
	assert.Contains(t, result, 30)
	assert.Contains(t, result, 45)
}

func TestParseField_List(t *testing.T) {
	result, err := parseField("1,3,5,7", 0, 59)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 3, 5, 7}, result)
}

func TestParseField_RangeWithStep(t *testing.T) {
	result, err := parseField("10-20/2", 0, 59)
	require.NoError(t, err)
	assert.Equal(t, []int{10, 12, 14, 16, 18, 20}, result)
}

func TestCronSchedule_Next(t *testing.T) {
	// Test "every minute"
	schedule, err := parseCronExpression("* * * * *")
	require.NoError(t, err)

	now := time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
	next := schedule.next(now)
	expected := time.Date(2024, 1, 1, 12, 31, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestCronSchedule_NextSpecificTime(t *testing.T) {
	// Test "at 2:30 AM every day"
	schedule, err := parseCronExpression("30 2 * * *")
	require.NoError(t, err)

	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	next := schedule.next(now)
	expected := time.Date(2024, 1, 2, 2, 30, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestCronSchedule_NextWeekday(t *testing.T) {
	// Test "9 AM on weekdays (Mon-Fri)"
	schedule, err := parseCronExpression("0 9 * * 1-5")
	require.NoError(t, err)

	// Start on Saturday
	now := time.Date(2024, 1, 6, 12, 0, 0, 0, time.UTC) // Saturday
	next := schedule.next(now)
	// Should be Monday at 9 AM
	assert.Equal(t, time.Monday, next.Weekday())
	assert.Equal(t, 9, next.Hour())
	assert.Equal(t, 0, next.Minute())
}

func TestCronSchedule_Matches(t *testing.T) {
	schedule, err := parseCronExpression("30 14 * * *")
	require.NoError(t, err)

	matches := schedule.matches(time.Date(2024, 1, 1, 14, 30, 0, 0, time.UTC))
	assert.True(t, matches)

	notMatches := schedule.matches(time.Date(2024, 1, 1, 14, 31, 0, 0, time.UTC))
	assert.False(t, notMatches)
}

func TestNewCronScheduler(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-queue")

	scheduler := NewCronScheduler(queue)
	assert.NotNil(t, scheduler)
	assert.False(t, scheduler.IsRunning())
}

func TestCronScheduler_AddJob(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-add")

	scheduler := NewCronScheduler(queue)

	err := scheduler.AddJob("daily-job", "0 9 * * *", "daily task")
	assert.NoError(t, err)

	jobs := scheduler.GetJobs()
	assert.Len(t, jobs, 1)
	assert.Equal(t, "daily-job", jobs[0].Name)
	assert.Equal(t, "0 9 * * *", jobs[0].Expression)
	assert.Equal(t, "daily task", jobs[0].Data)
}

func TestCronScheduler_AddJobDuplicate(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-dup")

	scheduler := NewCronScheduler(queue)

	err := scheduler.AddJob("job1", "* * * * *", "data")
	assert.NoError(t, err)

	err = scheduler.AddJob("job1", "* * * * *", "data")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestCronScheduler_AddJobInvalidCron(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-invalid")

	scheduler := NewCronScheduler(queue)

	err := scheduler.AddJob("job1", "invalid", "data")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cron expression")
}

func TestCronScheduler_RemoveJob(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-remove")

	scheduler := NewCronScheduler(queue)

	err := scheduler.AddJob("job1", "* * * * *", "data")
	require.NoError(t, err)

	err = scheduler.RemoveJob("job1")
	assert.NoError(t, err)

	jobs := scheduler.GetJobs()
	assert.Len(t, jobs, 0)
}

func TestCronScheduler_RemoveNonexistent(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-remove-none")

	scheduler := NewCronScheduler(queue)

	err := scheduler.RemoveJob("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestCronScheduler_StartStop(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-startstop")

	scheduler := NewCronScheduler(queue)

	assert.False(t, scheduler.IsRunning())

	err := scheduler.Start()
	assert.NoError(t, err)
	assert.True(t, scheduler.IsRunning())

	err = scheduler.Start()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already running")

	err = scheduler.Stop()
	assert.NoError(t, err)
	assert.False(t, scheduler.IsRunning())

	err = scheduler.Stop()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not running")
}

func TestCronScheduler_ExecuteJob(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-execute")

	// Empty the queue first
	queue.Empty()

	scheduler := NewCronScheduler(queue)

	// Add a job that runs every minute
	err := scheduler.AddJob("test-job", "* * * * *", "test-data")
	require.NoError(t, err)

	// Count processed jobs
	processed := int32(0)
	handler := func(job Job) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	queue.Process(handler, 1)

	// Start scheduler
	err = scheduler.Start()
	require.NoError(t, err)
	defer scheduler.Stop()

	// Wait up to 70 seconds for a job to be processed
	// (in case we just missed a minute boundary)
	maxWait := 70 * time.Second
	checkInterval := 100 * time.Millisecond
	elapsed := time.Duration(0)

	for elapsed < maxWait {
		if atomic.LoadInt32(&processed) > 0 {
			break
		}
		time.Sleep(checkInterval)
		elapsed += checkInterval
	}

	// At least one job should have been processed
	count := atomic.LoadInt32(&processed)
	assert.Greater(t, count, int32(0), "Expected at least one job to be processed")
}

func TestCronScheduler_WithOptions(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-options")

	scheduler := NewCronScheduler(queue)

	options := JobOptions{
		Priority: 10,
		Attempts: 5,
		Timeout:  60 * time.Second,
	}

	err := scheduler.AddJobWithOptions("priority-job", "* * * * *", "important", options)
	assert.NoError(t, err)

	jobs := scheduler.GetJobs()
	assert.Len(t, jobs, 1)
	assert.Equal(t, 10, jobs[0].Options.Priority)
	assert.Equal(t, 5, jobs[0].Options.Attempts)
}

func TestCronScheduler_MultipleJobs(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-multiple")

	scheduler := NewCronScheduler(queue)

	err := scheduler.AddJob("job1", "0 9 * * *", "morning")
	assert.NoError(t, err)

	err = scheduler.AddJob("job2", "0 17 * * *", "evening")
	assert.NoError(t, err)

	err = scheduler.AddJob("job3", "*/15 * * * *", "every 15 min")
	assert.NoError(t, err)

	jobs := scheduler.GetJobs()
	assert.Len(t, jobs, 3)
}

func TestCronScheduler_GetJobs(t *testing.T) {
	_, queue := setupCronTest(t, "test-cron-getjobs")

	scheduler := NewCronScheduler(queue)

	err := scheduler.AddJob("job1", "0 9 * * *", "data1")
	require.NoError(t, err)

	jobs := scheduler.GetJobs()
	assert.Len(t, jobs, 1)
	assert.Equal(t, "job1", jobs[0].Name)
	assert.NotZero(t, jobs[0].NextRun)
	assert.Zero(t, jobs[0].LastRun)
}

func TestParseField_EdgeCases(t *testing.T) {
	// Test empty string
	_, err := parseField("", 0, 59)
	assert.Error(t, err)

	// Test invalid range
	_, err = parseField("50-40", 0, 59)
	assert.Error(t, err)

	// Test out of bounds
	_, err = parseField("100", 0, 59)
	assert.Error(t, err)

	// Test negative
	_, err = parseField("-5", 0, 59)
	assert.Error(t, err)

	// Test invalid step
	_, err = parseField("*/0", 0, 59)
	assert.Error(t, err)

	// Test malformed step
	_, err = parseField("*//5", 0, 59)
	assert.Error(t, err)
}

func TestCronSchedule_ComplexExpressions(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		testTime   time.Time
		shouldMatch bool
	}{
		{
			"weekday morning",
			"0 9 * * 1-5",
			time.Date(2024, 1, 8, 9, 0, 0, 0, time.UTC), // Monday
			true,
		},
		{
			"weekday morning - weekend",
			"0 9 * * 1-5",
			time.Date(2024, 1, 6, 9, 0, 0, 0, time.UTC), // Saturday
			false,
		},
		{
			"every 15 minutes",
			"*/15 * * * *",
			time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC),
			true,
		},
		{
			"every 15 minutes - non-match",
			"*/15 * * * *",
			time.Date(2024, 1, 1, 12, 17, 0, 0, time.UTC),
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule, err := parseCronExpression(tt.expression)
			require.NoError(t, err)

			matches := schedule.matches(tt.testTime)
			assert.Equal(t, tt.shouldMatch, matches)
		})
	}
}
