package main

import (
	"math/rand"
	"time"
)

type RetryOptions struct {
	firstRetryDelayMs int
	maxRetryDelayMs   int
	jitterConstant    float64
}

func WithRetryAsync(retryOptions *RetryOptions, thunk func(success *bool) error) error {
	retryIn := retryOptions.firstRetryDelayMs
	maxRetryIn := retryOptions.maxRetryDelayMs
	jitterConstant := retryOptions.jitterConstant
	var err error
	success := false
	for !success && err == nil {
		err = thunk(&success)

		randomMultiplier := 1 - jitterConstant*(rand.Float64()-0.5)
		sleepTimeMs := time.Duration(randomMultiplier*float64(retryIn)) * time.Millisecond
		time.Sleep(sleepTimeMs)
		retryIn = min(2*retryIn, maxRetryIn)
	}
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
