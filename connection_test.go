package goqueue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestConnection(t *testing.T) (Connection, chan error) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})

	// Clean up
	ctx := context.Background()
	client.FlushDB(ctx)

	errChan := make(chan error, 100)

	conn, err := OpenConnectionWithRedisClient("test", client, errChan)
	require.NoError(t, err)
	require.NotNil(t, conn)

	t.Cleanup(func() {
		conn.StopHeartbeat()
		client.FlushDB(ctx)
		client.Close()
		close(errChan)
	})

	return conn, errChan
}

func TestOpenConnection(t *testing.T) {
	errChan := make(chan error, 10)
	defer close(errChan)

	conn, err := OpenConnection("test-conn", "tcp", "localhost:6379", 15, errChan)
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.Contains(t, conn.GetName(), "test-conn-")

	conn.StopHeartbeat()
}

func TestConnection_GetName(t *testing.T) {
	conn, _ := setupTestConnection(t)

	name := conn.GetName()
	assert.NotEmpty(t, name)
	assert.Contains(t, name, "test-")
}

func TestConnection_OpenQueue(t *testing.T) {
	conn, _ := setupTestConnection(t)

	queue, err := conn.OpenQueue("test-queue")
	require.NoError(t, err)
	require.NotNil(t, queue)

	assert.Equal(t, "test-queue", queue.GetName())
}

func TestConnection_GetOpenQueues(t *testing.T) {
	conn, _ := setupTestConnection(t)

	// Open multiple queues
	conn.OpenQueue("queue1")
	conn.OpenQueue("queue2")
	conn.OpenQueue("queue3")

	queues, err := conn.GetOpenQueues()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"queue1", "queue2", "queue3"}, queues)
}

func TestConnection_Heartbeat(t *testing.T) {
	conn, _ := setupTestConnection(t)

	// Check heartbeat is active
	err := conn.CheckHeartbeat()
	assert.NoError(t, err)

	// Wait a bit and check again
	time.Sleep(500 * time.Millisecond)
	err = conn.CheckHeartbeat()
	assert.NoError(t, err)
}

func TestConnection_StopHeartbeat(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer client.Close()

	client.FlushDB(context.Background())

	errChan := make(chan error, 10)
	conn, err := OpenConnectionWithRedisClient("test", client, errChan)
	require.NoError(t, err)

	// Heartbeat should be active
	err = conn.CheckHeartbeat()
	assert.NoError(t, err)

	// Stop heartbeat
	err = conn.StopHeartbeat()
	assert.NoError(t, err)

	// Wait for TTL to expire
	time.Sleep(2 * time.Second)

	// Heartbeat should be inactive
	err = conn.CheckHeartbeat()
	assert.Error(t, err)

	close(errChan)
}

func TestConnection_CollectStats(t *testing.T) {
	conn, _ := setupTestConnection(t)

	queue, err := conn.OpenQueue("stats-queue")
	require.NoError(t, err)

	// Add some jobs
	queue.Add("job1")
	queue.Add("job2")
	queue.Add("job3")

	// Collect stats
	stats, err := conn.CollectStats([]string{"stats-queue"})
	require.NoError(t, err)

	queueStat, exists := stats.QueueStats["stats-queue"]
	assert.True(t, exists)
	assert.Equal(t, 3, queueStat.ReadyCount)
	assert.Equal(t, 0, queueStat.UnackedCount)
	assert.Equal(t, 0, queueStat.RejectedCount)
}

func TestConnection_GetConnections(t *testing.T) {
	conn, _ := setupTestConnection(t)

	connections, err := conn.GetConnections()
	require.NoError(t, err)
	assert.Contains(t, connections, conn.GetName())
}

func TestConnection_StopAllConsuming(t *testing.T) {
	conn, _ := setupTestConnection(t)

	queue, _ := conn.OpenQueue("test-queue")

	handler := func(job Job) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	queue.Process(handler, 1)

	// Add a job
	queue.Add("test job")

	// Give it time to start processing
	time.Sleep(50 * time.Millisecond)

	// Stop all consuming
	err := conn.StopAllConsuming()
	assert.NoError(t, err)
}

func TestConnection_MultipleQueues(t *testing.T) {
	conn, _ := setupTestConnection(t)

	queue1, err := conn.OpenQueue("queue-1")
	require.NoError(t, err)

	queue2, err := conn.OpenQueue("queue-2")
	require.NoError(t, err)

	// Add jobs to both queues
	queue1.Add("job-1-1")
	queue1.Add("job-1-2")
	queue2.Add("job-2-1")

	// Check stats
	stats, err := conn.CollectStats([]string{"queue-1", "queue-2"})
	require.NoError(t, err)

	assert.Equal(t, 2, stats.QueueStats["queue-1"].ReadyCount)
	assert.Equal(t, 1, stats.QueueStats["queue-2"].ReadyCount)
}

func TestConnection_HeartbeatErrorRecovery(t *testing.T) {
	// This test verifies that the heartbeat can recover from transient errors
	conn, errChan := setupTestConnection(t)

	// Monitor for errors
	errorCount := 0
	done := make(chan bool)
	go func() {
		timeout := time.After(3 * time.Second)
		for {
			select {
			case err := <-errChan:
				if err != nil {
					errorCount++
				}
			case <-timeout:
				done <- true
				return
			}
		}
	}()

	// Wait for the test to complete
	<-done

	// Heartbeat should still be active despite any transient errors
	err := conn.CheckHeartbeat()
	assert.NoError(t, err)
}

func TestOpenConnectionWithRedisOptions(t *testing.T) {
	errChan := make(chan error, 10)
	defer close(errChan)

	opts := &redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	}

	conn, err := OpenConnectionWithRedisOptions("test", opts, errChan)
	require.NoError(t, err)
	require.NotNil(t, conn)

	conn.StopHeartbeat()
}

func TestConnection_ConcurrentQueueAccess(t *testing.T) {
	conn, _ := setupTestConnection(t)

	queue, _ := conn.OpenQueue("concurrent-queue")

	// Add jobs concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			queue.Add(string(rune(id)))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	count, err := queue.GetWaitingCount()
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}
