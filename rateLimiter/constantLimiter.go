package rateLimiter

import (
	"container/list"
	"time"
)

type RateLimiter struct {
	limit        int
	interval     time.Duration
	times        list.List
	forceDisable bool
}

// shouldRateLimit saves the now as time taken or returns an error if
// in the limit of rate limiting
func (r *RateLimiter) shouldRateLimit(now time.Time) bool {
	if r.times.Len() < r.limit {
		r.times.PushBack(now)
		return false
	}

	front := r.times.Front()
	if diff := now.Sub(front.Value.(time.Time)); diff < r.interval {
		return true
	}

	front.Value = now
	r.times.MoveToBack(front)
	return false
}
func (r *RateLimiter) RateWait() {
	if r.forceDisable {
		return
	}
	for {
		now := time.Now()
		if r.shouldRateLimit(now) {
			time.Sleep(time.Millisecond)
			//runtime.Gosched()
			continue
		}
		break
	}
}
