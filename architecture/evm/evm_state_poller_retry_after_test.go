package evm

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/stretchr/testify/require"
)

func init() { util.ConfigureTestLogger() }

func pollRateLimit(headers interface{}) error {
	return fmt.Errorf("wrapped: %w", &common.BaseError{
		Code:  common.ErrCodeEndpointCapacityExceeded,
		Cause: &common.BaseError{Details: map[string]interface{}{"headers": headers}},
	})
}

func TestPollRetryAfterParsing(t *testing.T) {
	for _, value := range []string{"", "bad", "0", "-1", "9223372036854775807", "Wed, 21 Oct 2015 07:28:00 GMT"} {
		t.Run(value, func(t *testing.T) {
			p, _, _ := newTestStatePoller(t, time.Hour, time.Millisecond)
			p.recordPollRetryAfter(pollRateLimit(map[string]string{"retry-after": value}))
			require.Zero(t, p.pollRetryAfter.Load())
		})
	}
	p, _, _ := newTestStatePoller(t, time.Hour, time.Millisecond)
	p.recordPollRetryAfter(&common.BaseError{Details: map[string]interface{}{"headers": map[string]string{"retry-after": "100"}}})
	require.Zero(t, p.pollRetryAfter.Load(), "other errors must not start cooldown")
	p.recordPollRetryAfter(pollRateLimit(util.ExtractUsefulHeaders(&http.Response{Header: http.Header{"Retry-After": {"120"}}})))
	require.InDelta(t, time.Now().Add(120*time.Second).Unix(), time.Unix(0, p.pollRetryAfter.Load()).Unix(), 1)
	first := p.pollRetryAfter.Load()
	p.recordPollRetryAfter(pollRateLimit(http.Header{"Retry-After": {"1"}}))
	require.Equal(t, first, p.pollRetryAfter.Load(), "concurrent shorter cooldown must not shorten the deadline")
	deadline := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	p.recordPollRetryAfter(pollRateLimit(http.Header{"Retry-After": {deadline.Format(http.TimeFormat)}}))
	require.Equal(t, deadline.UnixNano(), p.pollRetryAfter.Load())
}

type rateLimitedPollerUpstream struct{ *fakePollerUpstream }

func (f *rateLimitedPollerUpstream) Forward(ctx context.Context, req *common.NormalizedRequest, bypass, hedge bool) (*common.NormalizedResponse, error) {
	f.forwards.Add(1)
	return nil, pollRateLimit(util.ExtractUsefulHeaders(&http.Response{Header: http.Header{"Retry-After": {"3600"}}}))
}

func TestPollRetryAfterSuppressesOnlyLimitedUpstreamAndResumes(t *testing.T) {
	p, up, ctx := newTestStatePoller(t, time.Hour, time.Millisecond)
	p.upstream = &rateLimitedPollerUpstream{up}
	_, _, err := p.fetchBlock(ctx, "latest")
	require.Error(t, err)
	require.True(t, p.pollCoolingDown())
	before := up.forwards.Load()
	p.latestBlockShared.TryUpdate(ctx, 100)
	p.finalizedBlockShared.TryUpdate(ctx, 90)
	require.NoError(t, p.Poll(ctx))
	latest, err := p.PollLatestBlockNumber(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(100), latest)
	finalized, err := p.PollFinalizedBlockNumber(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(90), finalized)
	require.Equal(t, before, up.forwards.Load())
	other, otherUp, otherCtx := newTestStatePoller(t, time.Hour, time.Millisecond)
	_ = other.Poll(otherCtx)
	require.Positive(t, otherUp.forwards.Load(), "other upstreams must keep polling")
	p.pollRetryAfter.Store(time.Now().Add(-time.Second).UnixNano())
	_ = p.Poll(ctx)
	require.Greater(t, up.forwards.Load(), before, "polling must resume after expiry")
}
