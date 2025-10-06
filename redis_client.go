package goqueue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient is an interface that abstracts the Redis operations needed by the queue system
type RedisClient interface {
	// Basic key-value operations
	Set(key string, value interface{}, expiration time.Duration) (bool, error)
	Get(key string) (string, error)
	Del(key string) (int, error)
	TTL(key string) (time.Duration, error)

	// List operations
	LPush(key string, values ...interface{}) (int, error)
	LLen(key string) (int, error)
	LRem(key string, count int, value interface{}) (int, error)
	RPopLPush(source, destination string) (string, error)
	RPush(key string, values ...interface{}) (int, error)

	// Set operations
	SAdd(key string, members ...interface{}) (int, error)
	SMembers(key string) ([]string, error)
	SRem(key string, members ...interface{}) (int, error)
	SIsMember(key string, member interface{}) (bool, error)

	// Hash operations
	HSet(key string, field string, value interface{}) (bool, error)
	HGet(key string, field string) (string, error)
	HGetAll(key string) (map[string]string, error)
	HDel(key string, fields ...string) (int, error)

	// Sorted set operations
	ZAdd(key string, members ...redis.Z) (int, error)
	ZRem(key string, members ...interface{}) (int, error)
	ZRange(key string, start, stop int64) ([]string, error)
	ZRangeByScore(key string, min, max string) ([]string, error)

	// Scripting
	Eval(script string, keys []string, args ...interface{}) (interface{}, error)

	// Pipeline
	Pipeline() RedisPipeliner

	// Close the client
	Close() error
}

// RedisPipeliner is an interface for Redis pipeline operations
type RedisPipeliner interface {
	Set(key string, value interface{}, expiration time.Duration)
	Del(key string)
	LPush(key string, values ...interface{})
	RPush(key string, values ...interface{})
	SAdd(key string, members ...interface{})
	SRem(key string, members ...interface{})
	HSet(key string, field string, value interface{})
	ZAdd(key string, members ...redis.Z)
	Exec() error
}

// RedisWrapper wraps the go-redis client to implement the RedisClient interface
type RedisWrapper struct {
	redis.Cmdable
}

// Set sets a key-value pair with optional expiration
func (r RedisWrapper) Set(key string, value interface{}, expiration time.Duration) (bool, error) {
	ctx := context.Background()
	err := r.Cmdable.Set(ctx, key, value, expiration).Err()
	return err == nil, err
}

// Get retrieves the value for a key
func (r RedisWrapper) Get(key string) (string, error) {
	ctx := context.Background()
	val, err := r.Cmdable.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", ErrNotFound
	}
	return val, err
}

// Del deletes one or more keys
func (r RedisWrapper) Del(key string) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.Del(ctx, key).Result()
	return int(count), err
}

// TTL returns the time to live for a key
func (r RedisWrapper) TTL(key string) (time.Duration, error) {
	ctx := context.Background()
	ttl, err := r.Cmdable.TTL(ctx, key).Result()
	return ttl, err
}

// LPush inserts values at the head of a list
func (r RedisWrapper) LPush(key string, values ...interface{}) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.LPush(ctx, key, values...).Result()
	return int(count), err
}

// LLen returns the length of a list
func (r RedisWrapper) LLen(key string) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.LLen(ctx, key).Result()
	return int(count), err
}

// LRem removes elements from a list
func (r RedisWrapper) LRem(key string, count int, value interface{}) (int, error) {
	ctx := context.Background()
	removed, err := r.Cmdable.LRem(ctx, key, int64(count), value).Result()
	return int(removed), err
}

// RPopLPush atomically pops from one list and pushes to another
func (r RedisWrapper) RPopLPush(source, destination string) (string, error) {
	ctx := context.Background()
	val, err := r.Cmdable.RPopLPush(ctx, source, destination).Result()
	if err == redis.Nil {
		return "", ErrNotFound
	}
	return val, err
}

// RPush inserts values at the tail of a list
func (r RedisWrapper) RPush(key string, values ...interface{}) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.RPush(ctx, key, values...).Result()
	return int(count), err
}

// SAdd adds members to a set
func (r RedisWrapper) SAdd(key string, members ...interface{}) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.SAdd(ctx, key, members...).Result()
	return int(count), err
}

// SMembers returns all members of a set
func (r RedisWrapper) SMembers(key string) ([]string, error) {
	ctx := context.Background()
	members, err := r.Cmdable.SMembers(ctx, key).Result()
	return members, err
}

// SRem removes members from a set
func (r RedisWrapper) SRem(key string, members ...interface{}) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.SRem(ctx, key, members...).Result()
	return int(count), err
}

// SIsMember checks if a value is a member of a set
func (r RedisWrapper) SIsMember(key string, member interface{}) (bool, error) {
	ctx := context.Background()
	isMember, err := r.Cmdable.SIsMember(ctx, key, member).Result()
	return isMember, err
}

// HSet sets a field in a hash
func (r RedisWrapper) HSet(key string, field string, value interface{}) (bool, error) {
	ctx := context.Background()
	count, err := r.Cmdable.HSet(ctx, key, field, value).Result()
	return count > 0, err
}

// HGet gets a field from a hash
func (r RedisWrapper) HGet(key string, field string) (string, error) {
	ctx := context.Background()
	val, err := r.Cmdable.HGet(ctx, key, field).Result()
	if err == redis.Nil {
		return "", ErrNotFound
	}
	return val, err
}

// HGetAll gets all fields from a hash
func (r RedisWrapper) HGetAll(key string) (map[string]string, error) {
	ctx := context.Background()
	result, err := r.Cmdable.HGetAll(ctx, key).Result()
	return result, err
}

// HDel deletes fields from a hash
func (r RedisWrapper) HDel(key string, fields ...string) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.HDel(ctx, key, fields...).Result()
	return int(count), err
}

// ZAdd adds members to a sorted set
func (r RedisWrapper) ZAdd(key string, members ...redis.Z) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.ZAdd(ctx, key, members...).Result()
	return int(count), err
}

// ZRem removes members from a sorted set
func (r RedisWrapper) ZRem(key string, members ...interface{}) (int, error) {
	ctx := context.Background()
	count, err := r.Cmdable.ZRem(ctx, key, members...).Result()
	return int(count), err
}

// ZRange returns a range of members from a sorted set by index
func (r RedisWrapper) ZRange(key string, start, stop int64) ([]string, error) {
	ctx := context.Background()
	result, err := r.Cmdable.ZRange(ctx, key, start, stop).Result()
	return result, err
}

// ZRangeByScore returns a range of members from a sorted set by score
func (r RedisWrapper) ZRangeByScore(key string, min, max string) ([]string, error) {
	ctx := context.Background()
	result, err := r.Cmdable.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	return result, err
}

// Eval executes a Lua script
func (r RedisWrapper) Eval(script string, keys []string, args ...interface{}) (interface{}, error) {
	ctx := context.Background()
	result, err := r.Cmdable.Eval(ctx, script, keys, args...).Result()
	return result, err
}

// Pipeline creates a new pipeline
func (r RedisWrapper) Pipeline() RedisPipeliner {
	pipe := r.Cmdable.Pipeline()
	return &redisPipelineWrapper{pipe: pipe}
}

// Close closes the Redis client
func (r RedisWrapper) Close() error {
	if closer, ok := r.Cmdable.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// redisPipelineWrapper wraps the go-redis pipeline
type redisPipelineWrapper struct {
	pipe redis.Pipeliner
}

func (p *redisPipelineWrapper) Set(key string, value interface{}, expiration time.Duration) {
	ctx := context.Background()
	p.pipe.Set(ctx, key, value, expiration)
}

func (p *redisPipelineWrapper) Del(key string) {
	ctx := context.Background()
	p.pipe.Del(ctx, key)
}

func (p *redisPipelineWrapper) LPush(key string, values ...interface{}) {
	ctx := context.Background()
	p.pipe.LPush(ctx, key, values...)
}

func (p *redisPipelineWrapper) RPush(key string, values ...interface{}) {
	ctx := context.Background()
	p.pipe.RPush(ctx, key, values...)
}

func (p *redisPipelineWrapper) SAdd(key string, members ...interface{}) {
	ctx := context.Background()
	p.pipe.SAdd(ctx, key, members...)
}

func (p *redisPipelineWrapper) SRem(key string, members ...interface{}) {
	ctx := context.Background()
	p.pipe.SRem(ctx, key, members...)
}

func (p *redisPipelineWrapper) HSet(key string, field string, value interface{}) {
	ctx := context.Background()
	p.pipe.HSet(ctx, key, field, value)
}

func (p *redisPipelineWrapper) ZAdd(key string, members ...redis.Z) {
	ctx := context.Background()
	p.pipe.ZAdd(ctx, key, members...)
}

func (p *redisPipelineWrapper) Exec() error {
	ctx := context.Background()
	_, err := p.pipe.Exec(ctx)
	return err
}
