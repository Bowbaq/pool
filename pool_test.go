package pool_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/Bowbaq/pool"
)

func TestPoolSerial(t *testing.T) {
	i := 0
	worker := func(id uint, payload interface{}) interface{} {
		i++
		return i
	}
	p := pool.NewPool(1, worker)

	for expected := 1; expected <= 100; expected++ {
		req := pool.NewJob(struct{}{})
		p.Submit(req)
		if res := req.Result(); res != expected {
			t.Fatalf("got %#v; want %d", res, expected)
		}
	}
}

func TestPoolSerialLimitedWithoutBurst(t *testing.T) {
	n := uint(1000)
	op_per_sec := uint(10000)

	worker := func(id uint, payload interface{}) interface{} {
		return true
	}
	p := pool.NewRateLimitedPool(1, op_per_sec, 0, worker)

	done := uint(0)
	go func() {
		for j := uint(0); j < n; j++ {
			req := pool.NewJob(struct{}{})
			p.Submit(req)
			req.Result()
			done++
		}
	}()

	expected := float32(n) / float32(op_per_sec) * 1000
	<-time.After(time.Duration(expected-expected/10) * time.Millisecond)
	if done == n {
		t.Fatalf("Pool completed work faster than specified rate")
	}
}

func TestPoolSerialLimitedWithBurst(t *testing.T) {
	n := uint(1000)
	op_per_sec := uint(10000)

	worker := func(id uint, payload interface{}) interface{} {
		return true
	}
	p := pool.NewRateLimitedPool(1, op_per_sec, 1000, worker)

	done := uint(0)
	go func() {
		time.Sleep(100 * time.Millisecond)
		for j := uint(0); j < n; j++ {
			req := pool.NewJob(struct{}{})
			p.Submit(req)
			req.Result()
			done++
		}
	}()

	// Processing should happen immediately because we have enough burst capacity
	// to satisfy the jobs
	<-time.After(110 * time.Millisecond)
	if done != n {
		t.Fatalf("Pool didn't use burst capacity")
	}
}

func TestPoolParallel(t *testing.T) {
	testPoolParallel(t, 2, 100000)
	testPoolParallel(t, 5, 100000)
	testPoolParallel(t, 20, 100000)
	testPoolParallel(t, 50, 100000)
}

func TestPoolParallelLimitedWithoutBurst(t *testing.T) {
	testPoolParallelLimitedWithoutBurst(t, 2, 1000)
	testPoolParallelLimitedWithoutBurst(t, 5, 1000)
	testPoolParallelLimitedWithoutBurst(t, 20, 1000)
	testPoolParallelLimitedWithoutBurst(t, 50, 1000)
}

func testPoolParallel(t *testing.T, num_workers uint, num_req int) {
	runtime.GOMAXPROCS(4)

	worker := func(id uint, payload interface{}) interface{} {
		return id
	}
	p := pool.NewPool(num_workers, worker)

	ids := make(chan uint, num_req)
	go func() {
		for j := 0; j < num_req; j++ {
			req := pool.NewJob(struct{}{})
			p.Submit(req)
			worker_id := req.Result().(uint)
			ids <- worker_id
		}
		close(ids)
	}()

	stats := make(map[uint]uint)
	for id := range ids {
		if count, ok := stats[id]; ok {
			stats[id] = count + 1
		} else {
			stats[id] = 1
		}
	}

	fair := uint(num_req) / num_workers
	deviation := uint(num_req) / 100

	for id, count := range stats {
		if count < fair-deviation && count > fair+deviation {
			t.Fatalf("worker %d processed %d jobs, expected %d ± %d", id, count, fair, deviation)
		}
	}

	runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
}

func testPoolParallelLimitedWithoutBurst(t *testing.T, num_workers, num_req uint) {
	runtime.GOMAXPROCS(4)

	op_per_sec := uint(10000)

	worker := func(id uint, payload interface{}) interface{} {
		return true
	}
	p := pool.NewRateLimitedPool(num_workers, op_per_sec, 0, worker)

	done := uint(0)
	go func() {
		for j := uint(0); j < num_req; j++ {
			req := pool.NewJob(struct{}{})
			p.Submit(req)
			req.Result()
			done++
		}
	}()

	expected := float32(num_req) / float32(op_per_sec) * 1000
	<-time.After(time.Duration(expected-expected/10) * time.Millisecond)
	if done == num_req {
		t.Fatalf("Pool completed work faster than specified rate")
	}

	runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
}
