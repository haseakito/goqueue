package goqueue

import "errors"

var (
	ErrNotFound          = errors.New("not found")
	ErrQueuePaused       = errors.New("queue is paused")
	ErrMaxRetriesReached = errors.New("maximum retry attempts reached")
	ErrInvalidProgress   = errors.New("progress must be between 0 and 100")
	ErrJobTimeout        = errors.New("job execution timeout")
	ErrConnectionClosed  = errors.New("connection is closed")
)
