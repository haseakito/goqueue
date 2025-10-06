package goqueue

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CronScheduler manages scheduled jobs that run on a cron schedule
type CronScheduler interface {
	// AddJob adds a new cron job with a cron expression
	AddJob(name string, cronExpr string, data string) error

	// AddJobWithOptions adds a new cron job with custom job options
	AddJobWithOptions(name string, cronExpr string, data string, options JobOptions) error

	// RemoveJob removes a cron job by name
	RemoveJob(name string) error

	// Start starts the cron scheduler
	Start() error

	// Stop stops the cron scheduler
	Stop() error

	// IsRunning returns whether the scheduler is running
	IsRunning() bool

	// GetJobs returns all registered cron jobs
	GetJobs() []CronJob
}

// CronJob represents a scheduled job
type CronJob struct {
	Name       string
	Expression string
	Data       string
	Options    JobOptions
	NextRun    time.Time
	LastRun    time.Time
}

// cronScheduler implements the CronScheduler interface
type cronScheduler struct {
	queue      Queue
	jobs       map[string]*cronJobEntry
	mutex      sync.RWMutex
	running    bool
	stopChan   chan struct{}
	tickerChan <-chan time.Time
}

// cronJobEntry is an internal structure for managing cron jobs
type cronJobEntry struct {
	name       string
	expression string
	data       string
	options    JobOptions
	schedule   *cronSchedule
	nextRun    time.Time
	lastRun    time.Time
}

// cronSchedule represents a parsed cron expression
type cronSchedule struct {
	minute     []int
	hour       []int
	dayOfMonth []int
	month      []int
	dayOfWeek  []int
}

// NewCronScheduler creates a new cron scheduler for a queue
func NewCronScheduler(queue Queue) CronScheduler {
	return &cronScheduler{
		queue:    queue,
		jobs:     make(map[string]*cronJobEntry),
		running:  false,
		stopChan: make(chan struct{}),
	}
}

// AddJob adds a new cron job with default options
func (cs *cronScheduler) AddJob(name string, cronExpr string, data string) error {
	return cs.AddJobWithOptions(name, cronExpr, data, DefaultJobOptions())
}

// AddJobWithOptions adds a new cron job with custom options
func (cs *cronScheduler) AddJobWithOptions(name string, cronExpr string, data string, options JobOptions) error {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	// Check if job already exists
	if _, exists := cs.jobs[name]; exists {
		return fmt.Errorf("cron job '%s' already exists", name)
	}

	// Parse cron expression
	schedule, err := parseCronExpression(cronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	// Create job entry
	entry := &cronJobEntry{
		name:       name,
		expression: cronExpr,
		data:       data,
		options:    options,
		schedule:   schedule,
	}

	// Calculate next run time
	entry.nextRun = schedule.next(time.Now())

	cs.jobs[name] = entry
	return nil
}

// RemoveJob removes a cron job by name
func (cs *cronScheduler) RemoveJob(name string) error {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	if _, exists := cs.jobs[name]; !exists {
		return fmt.Errorf("cron job '%s' not found", name)
	}

	delete(cs.jobs, name)
	return nil
}

// Start starts the cron scheduler
func (cs *cronScheduler) Start() error {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	if cs.running {
		return fmt.Errorf("scheduler is already running")
	}

	cs.running = true
	cs.stopChan = make(chan struct{})

	// Create ticker for checking jobs every minute
	ticker := time.NewTicker(1 * time.Minute)
	cs.tickerChan = ticker.C

	go cs.run(ticker)
	return nil
}

// Stop stops the cron scheduler
func (cs *cronScheduler) Stop() error {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	if !cs.running {
		return fmt.Errorf("scheduler is not running")
	}

	cs.running = false
	close(cs.stopChan)
	return nil
}

// IsRunning returns whether the scheduler is running
func (cs *cronScheduler) IsRunning() bool {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.running
}

// GetJobs returns all registered cron jobs
func (cs *cronScheduler) GetJobs() []CronJob {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	jobs := make([]CronJob, 0, len(cs.jobs))
	for _, entry := range cs.jobs {
		jobs = append(jobs, CronJob{
			Name:       entry.name,
			Expression: entry.expression,
			Data:       entry.data,
			Options:    entry.options,
			NextRun:    entry.nextRun,
			LastRun:    entry.lastRun,
		})
	}
	return jobs
}

// run is the main loop for the scheduler
func (cs *cronScheduler) run(ticker *time.Ticker) {
	defer ticker.Stop()

	// Check immediately on start
	cs.checkAndRunJobs(time.Now())

	for {
		select {
		case <-cs.stopChan:
			return
		case now := <-cs.tickerChan:
			cs.checkAndRunJobs(now)
		}
	}
}

// checkAndRunJobs checks if any jobs need to run and executes them
func (cs *cronScheduler) checkAndRunJobs(now time.Time) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	// Truncate to minute precision for comparison
	now = now.Truncate(time.Minute)

	for _, entry := range cs.jobs {
		if entry.nextRun.Before(now) || entry.nextRun.Equal(now) {
			// Run the job
			go cs.executeJob(entry)

			// Update last run time
			entry.lastRun = now

			// Calculate next run time
			entry.nextRun = entry.schedule.next(now)
		}
	}
}

// executeJob executes a cron job by adding it to the queue
func (cs *cronScheduler) executeJob(entry *cronJobEntry) {
	_, err := cs.queue.AddWithOptions(entry.data, entry.options)
	if err != nil {
		// Log error (in production, you might want to use a proper logger)
		fmt.Printf("Error executing cron job '%s': %v\n", entry.name, err)
	}
}

// parseCronExpression parses a cron expression
// Format: minute hour day-of-month month day-of-week
// Supports: * (any), numbers, ranges (1-5), steps (*/5), lists (1,2,3)
func parseCronExpression(expr string) (*cronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields (minute hour day-of-month month day-of-week)")
	}

	minute, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("invalid minute field: %w", err)
	}

	hour, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("invalid hour field: %w", err)
	}

	dayOfMonth, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("invalid day-of-month field: %w", err)
	}

	month, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("invalid month field: %w", err)
	}

	dayOfWeek, err := parseField(fields[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("invalid day-of-week field: %w", err)
	}

	return &cronSchedule{
		minute:     minute,
		hour:       hour,
		dayOfMonth: dayOfMonth,
		month:      month,
		dayOfWeek:  dayOfWeek,
	}, nil
}

// parseField parses a single cron field
func parseField(field string, min, max int) ([]int, error) {
	// Handle wildcard
	if field == "*" {
		result := make([]int, max-min+1)
		for i := range result {
			result[i] = min + i
		}
		return result, nil
	}

	// Handle step values (e.g., */5, 10-20/2)
	if strings.Contains(field, "/") {
		parts := strings.Split(field, "/")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid step format: %s", field)
		}

		step, err := strconv.Atoi(parts[1])
		if err != nil || step <= 0 {
			return nil, fmt.Errorf("invalid step value: %s", parts[1])
		}

		var baseRange []int
		if parts[0] == "*" {
			baseRange = make([]int, max-min+1)
			for i := range baseRange {
				baseRange[i] = min + i
			}
		} else {
			baseRange, err = parseRange(parts[0], min, max)
			if err != nil {
				return nil, err
			}
		}

		var result []int
		for i := 0; i < len(baseRange); i += step {
			result = append(result, baseRange[i])
		}
		return result, nil
	}

	// Handle lists (e.g., 1,3,5)
	if strings.Contains(field, ",") {
		parts := strings.Split(field, ",")
		var result []int
		for _, part := range parts {
			values, err := parseRange(part, min, max)
			if err != nil {
				return nil, err
			}
			result = append(result, values...)
		}
		return result, nil
	}

	// Handle ranges (e.g., 1-5) or single values
	return parseRange(field, min, max)
}

// parseRange parses a range or single value
func parseRange(field string, min, max int) ([]int, error) {
	if strings.Contains(field, "-") {
		parts := strings.Split(field, "-")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid range format: %s", field)
		}

		start, err := strconv.Atoi(parts[0])
		if err != nil || start < min || start > max {
			return nil, fmt.Errorf("invalid range start: %s", parts[0])
		}

		end, err := strconv.Atoi(parts[1])
		if err != nil || end < min || end > max || end < start {
			return nil, fmt.Errorf("invalid range end: %s", parts[1])
		}

		result := make([]int, end-start+1)
		for i := range result {
			result[i] = start + i
		}
		return result, nil
	}

	// Single value
	value, err := strconv.Atoi(field)
	if err != nil || value < min || value > max {
		return nil, fmt.Errorf("invalid value: %s (must be between %d and %d)", field, min, max)
	}

	return []int{value}, nil
}

// next calculates the next run time for a cron schedule
func (cs *cronSchedule) next(from time.Time) time.Time {
	// Start from the next minute
	t := from.Add(time.Minute).Truncate(time.Minute)

	// Try up to 4 years (to handle leap years and edge cases)
	maxIterations := 366 * 24 * 60 * 4
	for i := 0; i < maxIterations; i++ {
		if cs.matches(t) {
			return t
		}
		t = t.Add(time.Minute)
	}

	// If we can't find a match in 4 years, something is wrong
	return from.Add(time.Hour * 24 * 365 * 100) // Far future
}

// matches checks if a time matches the cron schedule
func (cs *cronSchedule) matches(t time.Time) bool {
	// Check minute
	if !contains(cs.minute, t.Minute()) {
		return false
	}

	// Check hour
	if !contains(cs.hour, t.Hour()) {
		return false
	}

	// Check month
	if !contains(cs.month, int(t.Month())) {
		return false
	}

	// Check day of month and day of week
	// If both are restricted (not *), then either can match (OR logic)
	// If only one is restricted, it must match
	dayOfMonthMatches := contains(cs.dayOfMonth, t.Day())
	dayOfWeekMatches := contains(cs.dayOfWeek, int(t.Weekday()))

	// If both are wildcards, always match
	if len(cs.dayOfMonth) == 31 && len(cs.dayOfWeek) == 7 {
		return true
	}

	// If only day of month is restricted
	if len(cs.dayOfMonth) < 31 && len(cs.dayOfWeek) == 7 {
		return dayOfMonthMatches
	}

	// If only day of week is restricted
	if len(cs.dayOfMonth) == 31 && len(cs.dayOfWeek) < 7 {
		return dayOfWeekMatches
	}

	// If both are restricted, use OR logic (standard cron behavior)
	return dayOfMonthMatches || dayOfWeekMatches
}

// contains checks if a slice contains a value
func contains(slice []int, value int) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
