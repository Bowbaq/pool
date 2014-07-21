// Package pool provides a simple worker pool with optional throttling of job processing
package pool

import (
	"time"
)

// A Pool represents a set of background workers sharing a common work queue. Each worker is
// a goroutine and all workers attempt to receive jobs on a common channel. The pool can
// optionally be rate-limited to a set number of jobs / second, with configurable bursts.
type Pool struct {
	jobs   chan Job
	ticker *time.Ticker
}

// NewPool creates a new pool of n workers that process the queue as fast as possible, without
// rate limitation.
func NewPool(n uint, do Worker) *Pool {

	pool := Pool{
		jobs: make(chan Job, 2*n),
	}

	for i := uint(0); i < n; i++ {
		go func(id uint) {
			for job := range pool.jobs {
				result := do(id, job.payload)
				job.done <- result
			}
		}(i)
	}

	return &pool
}

// NewRateLimitedPool create a new Pool of n workers processing at most rate jobs / second.
// If burst > 1, up to burst tokens can accumulate letting the pool temporarily exceed the
// rate until the saved tokens are all consumed. Tokens accumulate when requests are submitted
// at a rate slower than the processing rate.
func NewRateLimitedPool(n, rate, burst uint, do Worker) *Pool {
	if rate == 0 {
		return NewPool(n, do)
	}

	pool := Pool{
		jobs:   make(chan Job, 2*n),
		ticker: time.NewTicker(time.Duration(1e9 / rate)),
	}

	// Make sure we have at least one slot in the channel to avoid contention
	if burst == 0 {
		burst = 1
	}

	limiter := make(chan time.Time, burst)
	go func() {
		for tick := range pool.ticker.C {
			limiter <- tick
		}
	}()

	for i := uint(0); i < n; i++ {
		go func(id uint) {
			for job := range pool.jobs {
				<-limiter
				result := do(id, job.payload)
				job.done <- result
			}
		}(i)
	}

	return &pool
}

// Submit a Job to be processed by the pool.
func (p *Pool) Submit(job Job) {
	p.jobs <- job
}

// Shutdown the pool. Currently queued jobs will be processed before terminating.
// Attempting to call Submit after Shutdown is an error.
func (p *Pool) Shutdown() {
	close(p.jobs)
	if p.ticker != nil {
		p.ticker.Stop()
	}
}

// A Worker processes a Job's payload. Once the worker returns, the result is made available
// to the Job.
type Worker func(id uint, payload interface{}) interface{}

// A Job is the basic unit of work for the pool.
type Job struct {
	payload interface{}
	done    chan interface{}
}

// NewJob creates a new Job with the given payload.
func NewJob(payload interface{}) Job {
	return Job{payload, make(chan interface{}, 1)}
}

// Result returns the result of a Job. This is a blocking call.
func (j Job) Result() interface{} {
	return <-j.done
}
