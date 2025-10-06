package goqueue

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomString(t *testing.T) {
	length := 10
	str := RandomString(length)

	assert.Len(t, str, length)
	assert.NotEmpty(t, str)
}

func TestRandomString_Uniqueness(t *testing.T) {
	// Generate multiple random strings and verify they're unique
	generated := make(map[string]bool)
	iterations := 100

	for i := 0; i < iterations; i++ {
		str := RandomString(16)
		assert.False(t, generated[str], "Generated duplicate random string: %s", str)
		generated[str] = true
	}

	assert.Len(t, generated, iterations)
}

func TestRandomString_DifferentLengths(t *testing.T) {
	lengths := []int{1, 5, 10, 20, 50, 100}

	for _, length := range lengths {
		str := RandomString(length)
		assert.Len(t, str, length, "Random string should have length %d", length)
	}
}

func TestGetTemplate_WithoutHashTags(t *testing.T) {
	template := "goqueue::queue::[{queue}]::ready"
	result := getTemplate(template, false)

	assert.NotContains(t, result, "[")
	assert.NotContains(t, result, "]")
	assert.Equal(t, "goqueue::queue::{queue}::ready", result)
}

func TestGetTemplate_WithHashTags(t *testing.T) {
	template := "goqueue::queue::[{queue}]::ready"
	result := getTemplate(template, true)

	assert.Contains(t, result, "[")
	assert.Contains(t, result, "]")
	assert.Equal(t, template, result)
}

func TestTemplateConstants(t *testing.T) {
	// Verify all templates contain the correct placeholders
	assert.Contains(t, connectionHeartbeatTemplate, phConnection)
	assert.Contains(t, connectionQueuesTemplate, phConnection)
	assert.Contains(t, connectionQueueConsumersBaseTemplate, phConnection)
	assert.Contains(t, connectionQueueConsumersBaseTemplate, phQueue)
	assert.Contains(t, connectionQueueUnackedBaseTemplate, phConnection)
	assert.Contains(t, connectionQueueUnackedBaseTemplate, phQueue)
	assert.Contains(t, queueReadyBaseTemplate, phQueue)
	assert.Contains(t, queueRejectedBaseTemplate, phQueue)
	assert.Contains(t, queueCompletedBaseTemplate, phQueue)
	assert.Contains(t, queueDelayedBaseTemplate, phQueue)
	assert.Contains(t, queuePausedBaseTemplate, phQueue)
}

func TestTemplateSubstitution(t *testing.T) {
	// Test that template substitution works correctly
	connectionName := "test-conn"
	queueName := "test-queue"

	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connectionName, 1)
	assert.Contains(t, heartbeatKey, connectionName)
	assert.NotContains(t, heartbeatKey, phConnection)

	readyKey := strings.Replace(queueReadyBaseTemplate, phQueue, queueName, 1)
	assert.Contains(t, readyKey, queueName)
	assert.NotContains(t, readyKey, phQueue)

	// Test nested replacement
	unackedKey := strings.Replace(connectionQueueUnackedBaseTemplate, phConnection, connectionName, 1)
	unackedKey = strings.Replace(unackedKey, phQueue, queueName, 1)
	assert.Contains(t, unackedKey, connectionName)
	assert.Contains(t, unackedKey, queueName)
	assert.NotContains(t, unackedKey, phConnection)
	assert.NotContains(t, unackedKey, phQueue)
}

func TestGlobalKeysConstant(t *testing.T) {
	assert.Equal(t, "goqueue::connections", connectionsKey)
}

func TestPlaceholderConstants(t *testing.T) {
	assert.Equal(t, "{connection}", phConnection)
	assert.Equal(t, "{queue}", phQueue)
	assert.Equal(t, "{consumer}", phConsumer)
}

func TestTemplatePrefix(t *testing.T) {
	// All templates should start with "goqueue::"
	templates := []string{
		connectionHeartbeatTemplate,
		connectionQueuesTemplate,
		connectionQueueConsumersBaseTemplate,
		connectionQueueUnackedBaseTemplate,
		queueReadyBaseTemplate,
		queueRejectedBaseTemplate,
		queueCompletedBaseTemplate,
		queueDelayedBaseTemplate,
		queuePausedBaseTemplate,
	}

	for _, template := range templates {
		assert.True(t, strings.HasPrefix(template, "goqueue::"),
			"Template should start with 'goqueue::': %s", template)
	}
}
