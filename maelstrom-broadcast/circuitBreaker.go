package main

import (
	"errors"
	"log"
	"time"
)

type CircuitState int

const (
	closed CircuitState = iota
	open
	halfOpen
)

var (
	// ErrTooManyRequests is returned when the CB state is half open and the requests count is over the cb maxRequests
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState is returned when the CB state is open
	ErrOpenState = errors.New("circuit breaker is open")
)

type CircularBuffer struct {
	buf []bool
	i   int
}

// return the element that was there before
func (circularBuffer *CircularBuffer) add(elem bool) bool {
	prev := circularBuffer.buf[circularBuffer.i]
	circularBuffer.buf[circularBuffer.i] = elem
	circularBuffer.i = (circularBuffer.i + 1) % len(circularBuffer.buf)
	return prev
}

type Counts struct {
	requests                uint32
	totalSuccesses          uint32
	totalFailures           uint32
	lastNRequests           *CircularBuffer // true is success, false is failure
	failuresInLastNRequests uint32
}

type CircuitBreakerOptions struct {
	resetTimeout time.Duration
}

func readyToTrip(counts *Counts) bool {
	return counts.requests >= 3
}

/** Non thread-safe. */
func CreateCircuitBreaker(options *CircuitBreakerOptions) func(thunk func(success *bool) error) func(success *bool) error {
	counts := &Counts{
		lastNRequests: &CircularBuffer{buf: make([]bool, 10, 10)},
	}
	for i := range counts.lastNRequests.buf {
		counts.lastNRequests.buf[i] = true
	}
	lastFailureTime := time.Time{}

	return func(thunk func(success *bool) error) func(success *bool) error {
		return decorate(thunk, counts, &lastFailureTime, options.resetTimeout)
	}
}

func decorate(thunk func(success *bool) error, counts *Counts, lastFailureTime *time.Time, resetTimeout time.Duration) func(success *bool) error {
	return func(success *bool) error {
		var err error
		state := currentState(counts, *lastFailureTime, resetTimeout)
		switch state {
		case closed, halfOpen:
			err = thunk(success)
			if err != nil {
				*lastFailureTime = time.Now()
				recordFailure(counts)
			} else {
				recordSuccess(counts)
			}
		case open:
			err = ErrOpenState
		}

		return err
	}
}

func recordFailure(counts *Counts) {
	counts.totalFailures++
	counts.failuresInLastNRequests++
	prev := counts.lastNRequests.add( /*success = */ false)
	if !prev {
		counts.failuresInLastNRequests++
	}
}

func recordSuccess(counts *Counts) {
	counts.totalSuccesses++
	prev := counts.lastNRequests.add( /*success = */ true)
	if !prev {
		counts.failuresInLastNRequests++
	}
}

func currentState(counts *Counts, lastFailureTime time.Time, resetTimeout time.Duration) CircuitState {
	failureRatio := float64(counts.failuresInLastNRequests) / float64(len(counts.lastNRequests.buf))
	// 0.6 could be a parameter here
	if failureRatio >= 0.6 && lastFailureTime.Add(resetTimeout).Before(time.Now()) {
		log.Println("CIRCUIT IS HALF-OPEN")
		log.Println("Failure ratio is: ", failureRatio)
		log.Println("lastFailureTime is: ", lastFailureTime)
		log.Println("Current time is: ", time.Now())
		return halfOpen
	} else if failureRatio >= 0.6 {
		log.Println("CIRCUIT IS OPEN")
		log.Println("Failure ratio is: ", failureRatio)
		log.Println("Current time is: ", time.Now())
		return open
	} else {
		log.Println("CIRCUIT IS CLOSED")
		log.Println("Failure ratio is: ", failureRatio)
		log.Println("Current time is: ", time.Now())
		return closed
	}
}
