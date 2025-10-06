package goqueue

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
)

// RandomString generates a random string of the specified length
func RandomString(length int) string {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(bytes)[:length]
}

// Redis key templates
const (
	// Placeholders
	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name

	// Global keys
	connectionsKey = "goqueue::connections" // set of connection names

	// Connection keys (templates)
	connectionHeartbeatTemplate          = "goqueue::connection::" + phConnection + "::heartbeat"                           // expires after heartbeatDuration
	connectionQueuesTemplate             = "goqueue::connection::" + phConnection + "::queues"                              // set of queues
	connectionQueueConsumersBaseTemplate = "goqueue::connection::" + phConnection + "::queue::[" + phQueue + "]::consumers" // set of consumers
	connectionQueueUnackedBaseTemplate   = "goqueue::connection::" + phConnection + "::queue::[" + phQueue + "]::unacked"   // list of unacked jobs
	queueReadyBaseTemplate               = "goqueue::queue::[" + phQueue + "]::ready"                                       // list of ready jobs
	queueRejectedBaseTemplate            = "goqueue::queue::[" + phQueue + "]::rejected"                                    // list of rejected jobs
	queueCompletedBaseTemplate           = "goqueue::queue::[" + phQueue + "]::completed"                                   // list of completed jobs
	queueDelayedBaseTemplate             = "goqueue::queue::[" + phQueue + "]::delayed"                                     // sorted set of delayed jobs
	queuePausedBaseTemplate              = "goqueue::queue::[" + phQueue + "]::paused"                                      // list of paused jobs
)

// getTemplate returns a template with or without Redis hash tags
func getTemplate(template string, useRedisHashTags bool) string {
	if useRedisHashTags {
		return template
	}
	return strings.ReplaceAll(strings.ReplaceAll(template, "[", ""), "]", "")
}
