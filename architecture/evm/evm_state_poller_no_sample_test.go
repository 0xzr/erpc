package evm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/util"
	"github.com/stretchr/testify/require"
)

func init() { util.ConfigureTestLogger() }

type samplePollerUpstream struct {
	*suggestGateUpstream
	result string
}

func (u *samplePollerUpstream) Forward(context.Context, *common.NormalizedRequest, bool, bool) (*common.NormalizedResponse, error) {
	return rawResultResponse(u.result), nil
}

type observedHeadCounter struct {
	data.CounterInt64SharedVariable
	invalidUpdate atomic.Bool
	refreshed     atomic.Bool
}

func (c *observedHeadCounter) TryUpdateIfStale(ctx context.Context, age time.Duration, fn func(context.Context) (int64, error)) (int64, error) {
	return c.CounterInt64SharedVariable.TryUpdateIfStale(ctx, age, func(ctx context.Context) (int64, error) {
		v, err := fn(ctx)
		c.refreshed.Store(true)
		if v == 0 && err == nil {
			c.invalidUpdate.Store(true)
		}
		return v, err
	})
}

func TestPollHeadNoSamplePreservesCounter(t *testing.T) {
	for _, finalized := range []bool{false, true} {
		for _, scenario := range []string{"empty", "unverified", "valid"} {
			t.Run(map[bool]string{false: "latest/", true: "finalized/"}[finalized]+scenario, func(t *testing.T) {
				up := &samplePollerUpstream{suggestGateUpstream: newSuggestGateUpstream(123, "123", nil), result: `{"number":"0x4c4b40","timestamp":"0x6553f100"}`}
				if scenario == "empty" {
					up.result = "null"
				}
				if scenario == "unverified" {
					up.chainErr = errors.New("temporary chain verification failure")
				}
				p := newGateTestPoller(t, up)
				p.debounceInterval = time.Nanosecond
				shared := p.latestBlockShared
				if finalized {
					shared = p.finalizedBlockShared
				}
				shared.TryUpdate(context.Background(), 1000)
				observed := &observedHeadCounter{CounterInt64SharedVariable: shared}
				var value int64
				var err error
				if finalized {
					p.finalizedBlockShared = observed
					value, err = p.PollFinalizedBlockNumber(context.Background())
				} else {
					p.latestBlockShared = observed
					value, err = p.PollLatestBlockNumber(context.Background())
				}
				require.NoError(t, err)
				require.True(t, observed.refreshed.Load())
				require.False(t, observed.invalidUpdate.Load(), "missing/rejected samples must never be submitted as valid zero heads")
				want := int64(1000)
				if scenario == "valid" {
					want = 5000000
				}
				require.Equal(t, want, value)
				require.Equal(t, want, shared.GetValue())
			})
		}
	}
}
