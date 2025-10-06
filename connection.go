package goqueue

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// NOTE: Be careful when changing any of these values.
	// Currently we update the heartbeat every second with a TTL of a minute.
	// This means that if we fail to update the heartbeat 60 times in a row
	// the connection might get cleaned up by a cleaner. So we want to set the
	// error limit to a value lower like this (like 45) to make sure we stop
	// all consuming before that happens.
	heartbeatDuration   = time.Minute // TTL of heartbeat key
	heartbeatInterval   = time.Second // how often we update the heartbeat key
	HeartbeatErrorLimit = 45          // stop consuming after this many heartbeat errors
)

type Connection interface {
	// OpenQueue opens a queue for producing/consuming jobs
	OpenQueue(name string) (Queue, error)

	// CollectStats returns stats for all queues
	CollectStats(queueList []string) (Stats, error)

	// GetOpenQueues returns the names of all open queues
	GetOpenQueues() ([]string, error)

	// StopHeartbeat stops the heartbeat goroutine
	StopHeartbeat() error

	// StopAllConsuming stops consuming on all queues
	StopAllConsuming() error

	// GetConnections returns all connection names
	GetConnections() ([]string, error)

	// CheckHeartbeat checks if the connection heartbeat is active
	CheckHeartbeat() error

	// GetName returns the connection name
	GetName() string

	// GetRedisClient returns the underlying Redis client
	GetRedisClient() RedisClient
}

// Stats contains statistics about the queue system
type Stats struct {
	QueueStats map[string]QueueStat
}

// QueueStat contains statistics for a single queue
type QueueStat struct {
	ReadyCount      int
	UnackedCount    int
	RejectedCount   int
	ConnectionCount int
}

type redisConnection struct {
	Name              string
	heartbeatKey      string
	queuesKey         string
	consumersTemplate string
	unackedTemplate   string
	readyTemplate     string
	rejectedTemplate  string
	completedTemplate string
	delayedTemplate   string
	pausedTemplate    string
	redisClient       RedisClient
	errChan           chan<- error
	heartbeatStop     chan chan struct{}
	openQueues        map[string]*redisQueue
	queueMutex        sync.WaitGroup
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag string, network string, address string, db int, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRedisOptions(tag, &redis.Options{Network: network, Addr: address, DB: db}, errChan)
}

// OpenConnectionWithRedisOptions allows you to pass more flexible options
func OpenConnectionWithRedisOptions(tag string, redisOption *redis.Options, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRedisClient(tag, redis.NewClient(redisOption), errChan)
}

// OpenConnectionWithRedisClient opens and returns a new connection
// This can be used to passa redis.ClusterClient.
func OpenConnectionWithRedisClient(tag string, redisClient redis.Cmdable, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithGoQuantRedisClient(tag, RedisWrapper{redisClient}, errChan)
}

// OpenConnectionWithRmqRedisClient: If you would like to use a redis client other than the ones
// supported in the constructors above, you can implement the RedisClient interface yourself
func OpenConnectionWithGoQuantRedisClient(tag string, redisClient RedisClient, errChan chan<- error) (Connection, error) {
	return openConnection(tag, redisClient, false, errChan)
}

func openConnection(tag string, redisClient RedisClient, useRedisHashTags bool, errChan chan<- error) (Connection, error) {
	name := fmt.Sprintf("%s-%s", tag, RandomString(6))

	connection := &redisConnection{
		Name:              name,
		heartbeatKey:      strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:         strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		consumersTemplate: getTemplate(connectionQueueConsumersBaseTemplate, useRedisHashTags),
		unackedTemplate:   getTemplate(connectionQueueUnackedBaseTemplate, useRedisHashTags),
		readyTemplate:     getTemplate(queueReadyBaseTemplate, useRedisHashTags),
		rejectedTemplate:  getTemplate(queueRejectedBaseTemplate, useRedisHashTags),
		completedTemplate: getTemplate(queueCompletedBaseTemplate, useRedisHashTags),
		delayedTemplate:   getTemplate(queueDelayedBaseTemplate, useRedisHashTags),
		pausedTemplate:    getTemplate(queuePausedBaseTemplate, useRedisHashTags),
		redisClient:       redisClient,
		errChan:           errChan,
		heartbeatStop:     make(chan chan struct{}, 1), // mark heartbeat as active, can be stopped
		openQueues:        make(map[string]*redisQueue),
	}

	if err := connection.updateHeartbeat(); err != nil { // checks the connection
		return nil, err
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	if _, err := redisClient.SAdd(connectionsKey, name); err != nil {
		return nil, err
	}

	go connection.heartbeat(errChan)
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection, nil
}

// GetName returns the connection name
func (c *redisConnection) GetName() string {
	return c.Name
}

// GetRedisClient returns the underlying Redis client
func (c *redisConnection) GetRedisClient() RedisClient {
	return c.redisClient
}

// OpenQueue opens a queue and adds it to the connection's queue set
func (c *redisConnection) OpenQueue(name string) (Queue, error) {
	queue := newQueue(name, c)
	if _, err := c.redisClient.SAdd(c.queuesKey, name); err != nil {
		return nil, err
	}
	return queue, nil
}

// GetOpenQueues returns a list of queue names currently open on this connection
func (c *redisConnection) GetOpenQueues() ([]string, error) {
	return c.redisClient.SMembers(c.queuesKey)
}

// StopAllConsuming stops consuming on all queues
func (c *redisConnection) StopAllConsuming() error {
	for _, queue := range c.openQueues {
		if err := queue.StopConsuming(); err != nil {
			return fmt.Errorf("failed to stop consuming on queue %s: %w", queue.name, err)
		}
	}
	return nil
}

// CollectStats collects statistics for the specified queues
func (c *redisConnection) CollectStats(queueList []string) (Stats, error) {
	stats := Stats{
		QueueStats: make(map[string]QueueStat),
	}

	for _, queueName := range queueList {
		queueStat := QueueStat{}

		// Get ready count
		readyKey := strings.Replace(c.readyTemplate, phQueue, queueName, 1)
		if count, err := c.redisClient.LLen(readyKey); err == nil {
			queueStat.ReadyCount = count
		}

		// Get rejected count
		rejectedKey := strings.Replace(c.rejectedTemplate, phQueue, queueName, 1)
		if count, err := c.redisClient.LLen(rejectedKey); err == nil {
			queueStat.RejectedCount = count
		}

		// Get unacked count across all connections
		connections, err := c.GetConnections()
		if err == nil {
			for _, connName := range connections {
				unackedKey := strings.Replace(c.unackedTemplate, phConnection, connName, 1)
				unackedKey = strings.Replace(unackedKey, phQueue, queueName, 1)
				if count, err := c.redisClient.LLen(unackedKey); err == nil {
					queueStat.UnackedCount += count
				}
			}
		}

		stats.QueueStats[queueName] = queueStat
	}

	return stats, nil
}

// GetConnections returns all active connection names
func (c *redisConnection) GetConnections() ([]string, error) {
	return c.redisClient.SMembers(connectionsKey)
}

// CheckHeartbeat checks if the heartbeat is still active
func (c *redisConnection) CheckHeartbeat() error {
	ttl, err := c.redisClient.TTL(c.heartbeatKey)
	if err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("heartbeat expired")
	}
	return nil
}

// StopHeartbeat stops the heartbeat goroutine
func (c *redisConnection) StopHeartbeat() error {
	finishChan := make(chan struct{})
	c.heartbeatStop <- finishChan
	<-finishChan
	return nil
}

// heartbeat maintains a heartbeat key in Redis to indicate the connection is alive
func (c *redisConnection) heartbeat(errChan chan<- error) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	errorCount := 0

	for {
		select {
		case <-ticker.C:
			if err := c.updateHeartbeat(); err != nil {
				errorCount++
				if errChan != nil {
					errChan <- err
				}

				// If we've had too many errors, stop consuming to prevent orphaned jobs
				if errorCount >= HeartbeatErrorLimit {
					if err := c.StopAllConsuming(); err != nil && errChan != nil {
						errChan <- fmt.Errorf("failed to stop consuming after heartbeat errors: %w", err)
					}
				}
			} else {
				errorCount = 0
			}

		case finishChan := <-c.heartbeatStop:
			if _, err := c.redisClient.Del(c.heartbeatKey); err != nil && errChan != nil {
				errChan <- err
			}
			close(finishChan)
			return
		}
	}
}

// updateHeartbeat sets the heartbeat key with a TTL
func (c *redisConnection) updateHeartbeat() error {
	_, err := c.redisClient.Set(c.heartbeatKey, "1", heartbeatDuration)
	return err
}
