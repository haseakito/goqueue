# Cron Scheduler Example

This example demonstrates how to use the GoQueue cron scheduler to schedule jobs using cron expressions.

## Overview

The cron scheduler allows you to schedule jobs to run at specific times or intervals using standard cron expressions. Jobs are automatically added to the queue at the scheduled times and processed by workers.

## Cron Expression Format

Cron expressions consist of 5 fields:

```
* * * * *
│ │ │ │ │
│ │ │ │ └─── Day of week (0-6, Sunday=0)
│ │ │ └───── Month (1-12)
│ │ └─────── Day of month (1-31)
│ └───────── Hour (0-23)
└─────────── Minute (0-59)
```

### Supported Syntax

- **Asterisk (`*`)**: Matches all values (e.g., `*` in minute field = every minute)
- **Number**: Specific value (e.g., `5` in hour field = 5 AM)
- **Range**: Inclusive range (e.g., `1-5` = 1, 2, 3, 4, 5)
- **List**: Comma-separated values (e.g., `1,3,5` = 1, 3, and 5)
- **Step**: Increments (e.g., `*/15` = every 15 units, `10-30/5` = 10, 15, 20, 25, 30)

### Example Expressions

| Expression | Description |
|------------|-------------|
| `* * * * *` | Every minute |
| `*/5 * * * *` | Every 5 minutes |
| `0 * * * *` | Every hour at minute 0 |
| `0 9 * * *` | Every day at 9:00 AM |
| `0 9 * * 1-5` | Every weekday at 9:00 AM |
| `0 0 * * 0` | Every Sunday at midnight |
| `30 14 1 * *` | 2:30 PM on the 1st of every month |
| `0 9-17 * * 1-5` | Every hour from 9 AM to 5 PM on weekdays |
| `*/10 9-17 * * *` | Every 10 minutes from 9 AM to 5 PM |

## Features Demonstrated

1. **Basic Cron Jobs**: Schedule jobs with simple cron expressions
2. **Custom Job Options**: Add jobs with priority, retry attempts, and timeouts
3. **Multiple Jobs**: Manage multiple scheduled jobs simultaneously
4. **Job Processing**: Process scheduled jobs with concurrent workers
5. **Graceful Shutdown**: Properly stop the scheduler and queue

## Running the Example

1. Make sure Redis is running:
   ```bash
   redis-server
   ```

2. Run the example:
   ```bash
   cd examples/cron
   go run main.go
   ```

3. The example will:
   - Register several cron jobs with different schedules
   - Start the scheduler
   - Process jobs as they're scheduled
   - Display job information when processed

4. Press `Ctrl+C` to gracefully shut down

## Code Walkthrough

### 1. Create a Cron Scheduler

```go
scheduler := goqueue.NewCronScheduler(queue)
```

### 2. Add Cron Jobs

```go
// Simple job - runs every minute
scheduler.AddJob("every-minute", "* * * * *", "Task data")

// Job with custom options
options := goqueue.JobOptions{
    Priority: 10,
    Attempts: 5,
    Timeout:  2 * time.Minute,
}
scheduler.AddJobWithOptions("priority-task", "*/10 * * * *", "Important task", options)
```

### 3. Start the Scheduler

```go
err := scheduler.Start()
if err != nil {
    log.Fatal(err)
}
```

### 4. Process Jobs

```go
handler := func(job goqueue.Job) error {
    fmt.Printf("Processing: %s\n", job.GetData())
    return nil
}

queue.Process(handler, 3) // 3 concurrent workers
```

### 5. Stop the Scheduler

```go
scheduler.Stop()
queue.StopConsuming()
```

## API Reference

### CronScheduler Methods

- `AddJob(name, cronExpr, data string) error` - Add a cron job with default options
- `AddJobWithOptions(name, cronExpr, data string, options JobOptions) error` - Add a cron job with custom options
- `RemoveJob(name string) error` - Remove a cron job by name
- `Start() error` - Start the scheduler
- `Stop() error` - Stop the scheduler
- `IsRunning() bool` - Check if scheduler is running
- `GetJobs() []CronJob` - Get all registered cron jobs

### CronJob Structure

```go
type CronJob struct {
    Name       string      // Job name
    Expression string      // Cron expression
    Data       string      // Job data
    Options    JobOptions  // Job options
    NextRun    time.Time   // Next scheduled run time
    LastRun    time.Time   // Last run time
}
```

## Notes

- The scheduler checks for jobs to run every minute
- Jobs are executed asynchronously when their scheduled time arrives
- Failed jobs can be retried based on the configured retry attempts
- The scheduler integrates seamlessly with the existing queue system
- Jobs maintain all standard queue features (priority, retry, progress tracking, etc.)
