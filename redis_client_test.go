package goqueue

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRedis(t *testing.T) RedisClient {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use separate DB for testing
	})

	wrapper := RedisWrapper{client}

	// Clean up test database before tests
	ctx := context.Background()
	client.FlushDB(ctx)

	t.Cleanup(func() {
		client.FlushDB(ctx)
		client.Close()
	})

	return wrapper
}

func TestRedisWrapper_SetGet(t *testing.T) {
	rc := setupTestRedis(t)

	// Test Set and Get
	ok, err := rc.Set("test-key", "test-value", 0)
	require.NoError(t, err)
	assert.True(t, ok)

	val, err := rc.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, "test-value", val)
}

func TestRedisWrapper_GetNotFound(t *testing.T) {
	rc := setupTestRedis(t)

	_, err := rc.Get("non-existent-key")
	assert.Equal(t, ErrNotFound, err)
}

func TestRedisWrapper_SetWithExpiration(t *testing.T) {
	rc := setupTestRedis(t)

	ok, err := rc.Set("expire-key", "value", 100*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, ok)

	// Should exist immediately
	val, err := rc.Get("expire-key")
	require.NoError(t, err)
	assert.Equal(t, "value", val)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	_, err = rc.Get("expire-key")
	assert.Equal(t, ErrNotFound, err)
}

func TestRedisWrapper_Del(t *testing.T) {
	rc := setupTestRedis(t)

	rc.Set("key-to-delete", "value", 0)
	count, err := rc.Del("key-to-delete")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	_, err = rc.Get("key-to-delete")
	assert.Equal(t, ErrNotFound, err)
}

func TestRedisWrapper_TTL(t *testing.T) {
	rc := setupTestRedis(t)

	rc.Set("ttl-key", "value", 10*time.Second)
	ttl, err := rc.TTL("ttl-key")
	require.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 10*time.Second)
}

func TestRedisWrapper_ListOperations(t *testing.T) {
	rc := setupTestRedis(t)

	// LPush
	count, err := rc.LPush("test-list", "a", "b", "c")
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// LLen
	length, err := rc.LLen("test-list")
	require.NoError(t, err)
	assert.Equal(t, 3, length)

	// RPush
	count, err = rc.RPush("test-list", "d")
	require.NoError(t, err)
	assert.Equal(t, 4, count)

	// LRem
	removed, err := rc.LRem("test-list", 1, "b")
	require.NoError(t, err)
	assert.Equal(t, 1, removed)

	length, err = rc.LLen("test-list")
	require.NoError(t, err)
	assert.Equal(t, 3, length)
}

func TestRedisWrapper_RPopLPush(t *testing.T) {
	rc := setupTestRedis(t)

	rc.RPush("source-list", "a", "b", "c")

	val, err := rc.RPopLPush("source-list", "dest-list")
	require.NoError(t, err)
	assert.Equal(t, "c", val)

	sourceLen, _ := rc.LLen("source-list")
	destLen, _ := rc.LLen("dest-list")
	assert.Equal(t, 2, sourceLen)
	assert.Equal(t, 1, destLen)
}

func TestRedisWrapper_SetOperations(t *testing.T) {
	rc := setupTestRedis(t)

	// SAdd
	count, err := rc.SAdd("test-set", "a", "b", "c")
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// SIsMember
	isMember, err := rc.SIsMember("test-set", "b")
	require.NoError(t, err)
	assert.True(t, isMember)

	// SMembers
	members, err := rc.SMembers("test-set")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, members)

	// SRem
	removed, err := rc.SRem("test-set", "b")
	require.NoError(t, err)
	assert.Equal(t, 1, removed)

	members, _ = rc.SMembers("test-set")
	assert.ElementsMatch(t, []string{"a", "c"}, members)
}

func TestRedisWrapper_HashOperations(t *testing.T) {
	rc := setupTestRedis(t)

	// HSet
	ok, err := rc.HSet("test-hash", "field1", "value1")
	require.NoError(t, err)
	assert.True(t, ok)

	rc.HSet("test-hash", "field2", "value2")

	// HGet
	val, err := rc.HGet("test-hash", "field1")
	require.NoError(t, err)
	assert.Equal(t, "value1", val)

	// HGetAll
	allFields, err := rc.HGetAll("test-hash")
	require.NoError(t, err)
	assert.Equal(t, 2, len(allFields))
	assert.Equal(t, "value1", allFields["field1"])
	assert.Equal(t, "value2", allFields["field2"])

	// HDel
	deleted, err := rc.HDel("test-hash", "field1")
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)

	_, err = rc.HGet("test-hash", "field1")
	assert.Equal(t, ErrNotFound, err)
}

func TestRedisWrapper_SortedSetOperations(t *testing.T) {
	rc := setupTestRedis(t)

	// ZAdd
	members := []redis.Z{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
		{Score: 3, Member: "c"},
	}
	count, err := rc.ZAdd("test-zset", members...)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// ZRange
	result, err := rc.ZRange("test-zset", 0, -1)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, result)

	// ZRangeByScore
	result, err = rc.ZRangeByScore("test-zset", "1", "2")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, result)

	// ZRem
	removed, err := rc.ZRem("test-zset", "b")
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
}

func TestRedisWrapper_Pipeline(t *testing.T) {
	rc := setupTestRedis(t)

	pipe := rc.Pipeline()
	pipe.Set("key1", "val1", 0)
	pipe.Set("key2", "val2", 0)
	pipe.LPush("list1", "item1", "item2")

	err := pipe.Exec()
	require.NoError(t, err)

	val1, _ := rc.Get("key1")
	val2, _ := rc.Get("key2")
	listLen, _ := rc.LLen("list1")

	assert.Equal(t, "val1", val1)
	assert.Equal(t, "val2", val2)
	assert.Equal(t, 2, listLen)
}

func TestRedisWrapper_Eval(t *testing.T) {
	rc := setupTestRedis(t)

	script := `return redis.call('SET', KEYS[1], ARGV[1])`
	_, err := rc.Eval(script, []string{"test-key"}, "test-value")
	require.NoError(t, err)

	val, err := rc.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, "test-value", val)
}
