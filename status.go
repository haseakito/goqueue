package goqueue

// JobStatus represents the current state of a job
type JobStatus string

const (
	StatusWaiting   JobStatus = "waiting"   // Job is in the ready queue
	StatusActive    JobStatus = "active"    // Job is being processed
	StatusCompleted JobStatus = "completed" // Job completed successfully
	StatusFailed    JobStatus = "failed"    // Job failed
	StatusDelayed   JobStatus = "delayed"   // Job is delayed (scheduled for future)
	StatusPaused    JobStatus = "paused"    // Job is paused
)

// String returns the string representation of JobStatus
func (s JobStatus) String() string {
	return string(s)
}
