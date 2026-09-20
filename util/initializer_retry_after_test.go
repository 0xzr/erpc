package util

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init() { ConfigureTestLogger() }

type retryAfterTestError struct{}

func (retryAfterTestError) Error() string { return "rate limited" }
func (retryAfterTestError) DeepSearch(key string) interface{} {
	if key == "headers" {
		return map[string]interface{}{"retry-after": "3600"}
	}
	return nil
}

func TestInitializerRetryAfterGatesAllRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	i := setupInitializer(t, ctx, &InitializerConfig{
		TaskTimeout: time.Second, AutoRetry: false,
		RetryMinDelay: time.Nanosecond, RetryMaxDelay: time.Nanosecond, RetryFactor: 1,
	})
	var calls atomic.Int32
	task := NewBootstrapTask("limited", func(context.Context) error {
		if calls.Add(1) == 1 {
			return fmt.Errorf("wrapped: %w", retryAfterTestError{})
		}
		return nil
	})
	require.Error(t, i.ExecuteTasks(ctx, task))
	require.Greater(t, task.retryAfter.Load(), time.Now().UnixNano())
	for _, respectBackoff := range []bool{false, true} {
		i.attemptRemainingTasks(respectBackoff)
		require.Equal(t, int32(1), calls.Load())
	}
	var healthy atomic.Bool
	_ = i.ExecuteTasks(ctx, NewBootstrapTask("healthy", func(context.Context) error {
		healthy.Store(true)
		return nil
	}))
	require.True(t, healthy.Load())
	task.retryAfter.Store(time.Now().Add(-time.Second).UnixNano())
	i.attemptRemainingTasks(false)
	require.NoError(t, i.WaitForTasks(ctx))
	require.Equal(t, int32(2), calls.Load())
}
