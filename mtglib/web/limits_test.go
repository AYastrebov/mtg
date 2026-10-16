package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultLimitsValidate(t *testing.T) {
	require.NoError(t, DefaultLimits().validate())
}

func TestDefaultTimeouts(t *testing.T) {
	to := DefaultTimeouts()
	assert.Equal(t, 25*time.Second, to.LongPoll)
	assert.Equal(t, 2*time.Minute, to.ReconnectGrace)
	assert.Equal(t, 2*time.Minute, to.BootstrapLifetime)
	assert.Equal(t, 15*time.Second, to.Shutdown)
}

func TestLimitsValidateRejects(t *testing.T) {
	tests := map[string]func(*Limits){
		"batch too small":   func(l *Limits) { l.CarrierBatchBytes = 100 << 10 },
		"batch above body":  func(l *Limits) { l.MaxBodyBytes = 512 << 10; l.CarrierBatchBytes = 1 << 20 },
		"batch above 2MiB":  func(l *Limits) { l.MaxBodyBytes = 4 << 20; l.CarrierBatchBytes = 3 << 20 },
		"reserve bytes":     func(l *Limits) { l.MaxSessions = 100000 },
		"reserve items":     func(l *Limits) { l.MaxPendingItemsGlobal = 1000 },
		"frame payload big": func(l *Limits) { l.MaxFramePayload = 2 << 20 },
		"frame payload 0":   func(l *Limits) { l.MaxFramePayload = 0 },
		"zero streams":      func(l *Limits) { l.MaxStreams = 0 },
		"negative per ip":   func(l *Limits) { l.MaxSessionsPerIP = -1 },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			l := DefaultLimits()
			mutate(&l)
			err := l.validate()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrLimits)
		})
	}
}

func TestLimitsAllowZeroPerIP(t *testing.T) {
	l := DefaultLimits()
	l.MaxSessionsPerIP, l.MaxBootstrapsPerIP = 0, 0
	assert.NoError(t, l.validate())
}

func TestReserves(t *testing.T) {
	l := DefaultLimits()
	assert.Equal(t, 16+3*128, l.controlReserveItems())
	assert.Equal(t, int64(16+3*128)*(queueItemCost+FrameHeaderSize+4), l.controlReserveBytes())
	assert.Equal(t, int64(2<<20+4096*queueItemCost), l.uplinkReserveBytes())
}

func TestMergeOnlyLowers(t *testing.T) {
	g := DefaultLimits()

	m := g.merge(ProfileLimits{MaxStreams: 10, MaxPendingPerSession: 1024})
	assert.Equal(t, 10, m.MaxStreams)
	assert.Equal(t, int64(1024), m.MaxPendingPerSession)
	assert.Equal(t, g.MaxSessions, m.MaxSessions)

	m = g.merge(ProfileLimits{MaxStreams: 99999, MaxPendingPerSession: 1 << 40})
	assert.Equal(t, g.MaxStreams, m.MaxStreams)
	assert.Equal(t, g.MaxPendingPerSession, m.MaxPendingPerSession)

	assert.Equal(t, g, g.merge(ProfileLimits{}))
}

func TestRateBucket(t *testing.T) {
	now := time.Unix(1000, 0)
	b := newRateBucket(60, 2, now)

	assert.True(t, b.take(now))
	assert.True(t, b.take(now))
	assert.False(t, b.take(now))

	now = now.Add(time.Second)
	assert.True(t, b.take(now))
	assert.False(t, b.take(now))

	now = now.Add(time.Minute)
	assert.True(t, b.take(now))
	assert.True(t, b.take(now))
	assert.False(t, b.take(now))
}
