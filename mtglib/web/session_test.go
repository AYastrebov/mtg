package web

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/essentials"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testProfileIndex = 3

var errFakeServe = errors.New("fake serve error")

type fakeSink struct {
	mu sync.Mutex

	bytes [3]int64
	items [3]int

	refuse        [3]bool
	refuseAcquire bool

	acquired int
	released int
}

func (f *fakeSink) changePending(class pendingClass, bytes int64, items int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if (bytes > 0 || items > 0) && f.refuse[class] {
		return false
	}

	f.bytes[class] += bytes
	f.items[class] += items

	return true
}

func (f *fakeSink) acquireStream(profileIdx int, _ time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if profileIdx != testProfileIndex || f.refuseAcquire {
		return false
	}

	f.acquired++

	return true
}

func (f *fakeSink) releaseStream(profileIdx int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if profileIdx == testProfileIndex {
		f.released++
	}
}

func (f *fakeSink) setRefuse(class pendingClass, v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.refuse[class] = v
}

func (f *fakeSink) setRefuseAcquire(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.refuseAcquire = v
}

func (f *fakeSink) totals() (bytes [3]int64, items [3]int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.bytes, f.items
}

func (f *fakeSink) streams() (acquired, released int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.acquired, f.released
}

type fakeServe struct {
	mu      sync.Mutex
	conns   []essentials.Conn
	idx     []int
	err     error
	block   chan struct{}
	entered chan struct{}
}

func (f *fakeServe) serve(conn essentials.Conn, secretIdx int) error {
	f.mu.Lock()
	block, entered := f.block, f.entered
	f.mu.Unlock()

	if entered != nil {
		entered <- struct{}{}
	}

	if block != nil {
		<-block
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	f.conns = append(f.conns, conn)
	f.idx = append(f.idx, secretIdx)

	return nil
}

func (f *fakeServe) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.conns)
}

func (f *fakeServe) conn(t *testing.T, i int) essentials.Conn {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	require.Greater(t, len(f.conns), i)

	return f.conns[i]
}

type sessionHarness struct {
	s     *session
	sink  *fakeSink
	serve *fakeServe
}

func newTestSession(t *testing.T, mutate func(*Limits)) *sessionHarness {
	t.Helper()

	limits := DefaultLimits()
	if mutate != nil {
		mutate(&limits)
	}

	h := &sessionHarness{sink: &fakeSink{}, serve: &fakeServe{}}
	h.s = newSession(&Profile{Index: testProfileIndex}, limits, h.sink, h.serve.serve,
		net.IPv4(10, 0, 0, 1), logger.NewNoopLogger(), time.Now)

	t.Cleanup(func() {
		h.s.close()
		h.s.wait()
	})

	return h
}

func batch(frames ...[]byte) []byte {
	return bytes.Join(frames, nil)
}

func openFrame(id uint32) []byte  { return EncodeFrame(FrameOpen, id, nil) }
func closeFrame(id uint32) []byte { return EncodeFrame(FrameClose, id, nil) }
func dataFrame(id uint32, p []byte) []byte {
	return EncodeFrame(FrameData, id, p)
}

func windowFrame(id, n uint32) []byte {
	return EncodeFrame(FrameWindow, id, WindowPayload(n))
}

// pendingFrames returns a copy of the pending downlink queue.
func pendingFrames(s *session) []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()

	rv := make([]Frame, 0, len(s.downlink))
	for _, it := range s.downlink {
		rv = append(rv, Frame{
			Type:     it.frame.Type,
			StreamID: it.frame.StreamID,
			Payload:  append([]byte(nil), it.frame.Payload...),
		})
	}

	return rv
}

func hasFrame(s *session, t FrameType, id uint32) bool {
	for _, f := range pendingFrames(s) {
		if f.Type == t && f.StreamID == id {
			return true
		}
	}

	return false
}

func windowSum(s *session, id uint32) uint32 {
	var sum uint32

	for _, f := range pendingFrames(s) {
		if f.Type == FrameWindow && f.StreamID == id {
			n, _ := WindowAmount(f.Payload)
			sum += n
		}
	}

	return sum
}

func dataFrames(s *session, id uint32) (frames []Frame, total int) {
	for _, f := range pendingFrames(s) {
		if f.Type == FrameData && f.StreamID == id {
			frames = append(frames, f)
			total += len(f.Payload)
		}
	}

	return frames, total
}

// drainDownlink simulates Task 8's batch taker + ack: it pops every
// pending item and releases its budget.
func drainDownlink(s *session) {
	s.mu.Lock()
	items := s.popDownlinkLocked(len(s.downlink))
	s.mu.Unlock()

	for _, it := range items {
		s.releaseDownlink(it.class, it.bytes, it.items)
	}
}

func sendCredit(s *session, id uint32) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st := s.streams[id]; st != nil {
		return st.sendCredit
	}

	return -1
}

func isClosed(s *session) bool {
	select {
	case <-s.closed():
		return true
	default:
		return false
	}
}

func readN(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()

	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))

	buf := make([]byte, n)
	_, err := io.ReadFull(c, buf)
	require.NoError(t, err)

	return buf
}

func assertNoRead(t *testing.T, c net.Conn) {
	t.Helper()

	require.NoError(t, c.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

	n, err := c.Read(make([]byte, 1))
	assert.Equal(t, 0, n)

	var ne net.Error
	assert.True(t, errors.As(err, &ne) && ne.Timeout(), "expected timeout, got %v", err)
}

func TestSessionOpenServesStream(t *testing.T) {
	h := newTestSession(t, nil)

	ack, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)
	assert.EqualValues(t, 1, ack)
	require.Equal(t, 1, h.serve.count())
	assert.Equal(t, []int{testProfileIndex}, h.serve.idx)

	ack, err = h.s.processUp(2, dataFrame(7, []byte("abc")))
	require.NoError(t, err)
	assert.EqualValues(t, 2, ack)

	assert.Equal(t, "abc", string(readN(t, h.serve.conn(t, 0), 3)))
	require.Eventually(t, func() bool { return windowSum(h.s, 7) == 3 },
		time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		b, i := h.sink.totals()

		return b[pendingUplink] == 0 && i[pendingUplink] == 0
	}, time.Second, time.Millisecond)
}

func TestSessionRetryIdempotent(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	body := dataFrame(7, []byte("abc"))

	for range 2 {
		ack, err := h.s.processUp(2, body)
		require.NoError(t, err)
		assert.EqualValues(t, 2, ack)
	}

	conn := h.serve.conn(t, 0)
	assert.Equal(t, "abc", string(readN(t, conn, 3)))
	assertNoRead(t, conn)

	_, err = h.s.processUp(2, dataFrame(7, []byte("xyz")))
	require.ErrorIs(t, err, errSessionProtocol)

	select {
	case <-h.s.closed():
	case <-time.After(time.Second):
		t.Fatal("session is not closed")
	}

	_, err = h.s.processUp(3, dataFrame(7, []byte("xyz")))
	require.ErrorIs(t, err, errSessionClosed)
}

func TestSessionSequenceErrors(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	_, err = h.s.processUp(3, openFrame(8))
	require.ErrorIs(t, err, errSessionProtocol)
	assert.True(t, isClosed(h.s))

	h2 := newTestSession(t, nil)
	_, err = h2.s.processUp(0, openFrame(7))
	require.ErrorIs(t, err, errSessionProtocol)
	assert.True(t, isClosed(h2.s))
}

func TestSessionMalformedBatchIsProtocolError(t *testing.T) {
	for name, body := range map[string][]byte{
		"empty":       nil,
		"truncated":   openFrame(7)[:5],
		"bad shape":   EncodeFrame(FrameOpen, 0, nil),
		"hello":       EncodeFrame(FrameHello, 0, []byte{1}),
		"zero window": EncodeFrame(FrameWindow, 7, WindowPayload(0)),
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestSession(t, nil)
			_, err := h.s.processUp(1, body)
			require.ErrorIs(t, err, errSessionProtocol)
			assert.True(t, isClosed(h.s))
		})
	}
}

func TestSessionWindowExceeded(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	chunk := make([]byte, MaxFramePayload)
	frames := make([][]byte, 0, 5)

	for range InitialStreamWindow / MaxFramePayload {
		frames = append(frames, dataFrame(7, chunk))
	}

	frames = append(frames, dataFrame(7, []byte{1}))

	_, err = h.s.processUp(2, batch(frames...))
	require.ErrorIs(t, err, errSessionProtocol)
	assert.True(t, isClosed(h.s))

	conn := h.serve.conn(t, 0)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second)) // fails once the peer is closed
	n, err := conn.Read(make([]byte, 1))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF)

	b, i := h.sink.totals()
	assert.Equal(t, [3]int64{}, b)
	assert.Equal(t, [3]int{}, i)
}

func TestSessionWindowExactlyFits(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	chunk := make([]byte, MaxFramePayload)
	frames := make([][]byte, 0, 4)

	for range InitialStreamWindow / MaxFramePayload {
		frames = append(frames, dataFrame(7, chunk))
	}

	_, err = h.s.processUp(2, batch(frames...))
	require.NoError(t, err)

	// The window is exhausted until the proxy side consumes data.
	_, err = h.s.processUp(3, dataFrame(7, []byte{1}))
	require.ErrorIs(t, err, errSessionProtocol)
}

func TestSessionUnknownStreamIsProtocolError(t *testing.T) {
	for name, body := range map[string][]byte{
		"data":   dataFrame(9, []byte("x")),
		"window": windowFrame(9, 1),
		"close":  closeFrame(9),
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestSession(t, nil)
			_, err := h.s.processUp(1, body)
			require.ErrorIs(t, err, errSessionProtocol)
			assert.True(t, isClosed(h.s))
		})
	}
}

func TestSessionValidationIsAtomic(t *testing.T) {
	h := newTestSession(t, nil)

	// OPEN 7 is valid but DATA 9 is not: nothing must be applied.
	_, err := h.s.processUp(1, batch(openFrame(7), dataFrame(9, []byte("x"))))
	require.ErrorIs(t, err, errSessionProtocol)
	assert.Equal(t, 0, h.serve.count())

	acquired, _ := h.sink.streams()
	assert.Equal(t, 0, acquired)
}

func TestSessionTombstoneSilences(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	_, err = h.s.processUp(2, closeFrame(7))
	require.NoError(t, err)

	conn := h.serve.conn(t, 0)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second)) // fails once the peer is closed
	_, err = conn.Read(make([]byte, 1))
	assert.ErrorIs(t, err, io.EOF)

	_, released := h.sink.streams()
	assert.Equal(t, 1, released)
	assert.False(t, hasFrame(h.s, FrameClose, 7), "client CLOSE must not be echoed")

	ack, err := h.s.processUp(3, batch(dataFrame(7, []byte("x")), windowFrame(7, 5), closeFrame(7)))
	require.NoError(t, err)
	assert.EqualValues(t, 3, ack)
	assert.False(t, isClosed(h.s))

	b, i := h.sink.totals()
	assert.Zero(t, b[pendingUplink])
	assert.Zero(t, i[pendingUplink])

	_, err = h.s.processUp(4, openFrame(7))
	require.ErrorIs(t, err, errSessionProtocol)
}

func TestSessionTombstoneRing(t *testing.T) {
	h := newTestSession(t, func(l *Limits) { l.MaxClosedStreamIDs = 2 })

	_, err := h.s.processUp(1, batch(openFrame(1), closeFrame(1), openFrame(2), closeFrame(2),
		openFrame(3), closeFrame(3)))
	require.NoError(t, err)

	// Streams closed within the same batch are never served.
	assert.Equal(t, 0, h.serve.count())

	acquired, released := h.sink.streams()
	assert.Equal(t, 3, acquired)
	assert.Equal(t, 3, released)

	// id 1 was evicted from the ring: it is unknown again.
	_, err = h.s.processUp(2, batch(dataFrame(3, []byte("x")), openFrame(1)))
	require.NoError(t, err)
	assert.Equal(t, 1, h.serve.count())
}

func TestSessionOpenAndCloseInOneBatch(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, batch(openFrame(7), dataFrame(7, []byte("x")), closeFrame(7),
		dataFrame(7, []byte("y"))))
	require.NoError(t, err)

	_, err = h.s.processUp(2, openFrame(7))
	require.ErrorIs(t, err, errSessionProtocol)
}

func TestSessionOpenRefusedGetsClose(t *testing.T) {
	t.Run("sink", func(t *testing.T) {
		h := newTestSession(t, nil)
		h.sink.setRefuseAcquire(true)

		ack, err := h.s.processUp(1, batch(openFrame(7), dataFrame(7, []byte("x"))))
		require.NoError(t, err)
		assert.EqualValues(t, 1, ack)
		assert.True(t, hasFrame(h.s, FrameClose, 7))
		assert.Equal(t, 0, h.serve.count())
		assert.False(t, isClosed(h.s))

		b, i := h.sink.totals()
		assert.Zero(t, b[pendingUplink])
		assert.Zero(t, i[pendingUplink])
		assert.EqualValues(t, queueItemCost, b[pendingControl])
		assert.Equal(t, 1, i[pendingControl])

		// The refused id is tombstoned.
		_, err = h.s.processUp(2, openFrame(7))
		require.ErrorIs(t, err, errSessionProtocol)
	})

	t.Run("per session", func(t *testing.T) {
		h := newTestSession(t, func(l *Limits) { l.MaxStreamsPerSession = 1 })

		_, err := h.s.processUp(1, batch(openFrame(7), openFrame(8)))
		require.NoError(t, err)
		assert.Equal(t, 1, h.serve.count())
		assert.True(t, hasFrame(h.s, FrameClose, 8))
		assert.False(t, hasFrame(h.s, FrameClose, 7))
		assert.False(t, isClosed(h.s))
	})

	t.Run("serve error", func(t *testing.T) {
		h := newTestSession(t, nil)
		h.serve.err = errFakeServe

		_, err := h.s.processUp(1, openFrame(7))
		require.NoError(t, err)
		assert.True(t, hasFrame(h.s, FrameClose, 7))

		acquired, released := h.sink.streams()
		assert.Equal(t, 1, acquired)
		assert.Equal(t, 1, released)
		assert.False(t, isClosed(h.s))

		_, err = h.s.processUp(2, dataFrame(7, []byte("x")))
		require.NoError(t, err)
	})

	t.Run("control refused closes session", func(t *testing.T) {
		h := newTestSession(t, nil)
		h.sink.setRefuseAcquire(true)
		h.sink.setRefuse(pendingControl, true)

		_, err := h.s.processUp(1, openFrame(7))
		require.ErrorIs(t, err, errSessionClosed)
		assert.True(t, isClosed(h.s))
	})
}

func TestSessionBackpressure503(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	h.sink.setRefuse(pendingUplink, true)

	body := dataFrame(7, []byte("abc"))
	_, err = h.s.processUp(2, body)
	require.ErrorIs(t, err, errBackpressure)
	assert.False(t, isClosed(h.s))

	h.s.mu.Lock()
	st := h.s.streams[7]
	assert.EqualValues(t, InitialStreamWindow, st.receiveWindow)
	assert.Empty(t, st.upQueue)
	h.s.mu.Unlock()

	conn := h.serve.conn(t, 0)
	assertNoRead(t, conn)

	h.sink.setRefuse(pendingUplink, false)

	ack, err := h.s.processUp(2, body)
	require.NoError(t, err)
	assert.EqualValues(t, 2, ack)
	assert.Equal(t, "abc", string(readN(t, conn, 3)))
}

func TestSessionBackpressureSessionBudget(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	limits := h.s.limits
	h.s.mu.Lock()
	// Pretend the data partition is almost full.
	h.s.pendingBytes[pendingDownlink] = limits.MaxPendingPerSession - limits.controlReserveBytes() - 10
	h.s.mu.Unlock()

	_, err = h.s.processUp(2, dataFrame(7, []byte("abc")))
	require.ErrorIs(t, err, errBackpressure)

	h.s.mu.Lock()
	h.s.pendingBytes[pendingDownlink] = 0
	h.s.mu.Unlock()
}

func TestSessionConcurrentUp(t *testing.T) {
	h := newTestSession(t, nil)
	h.serve.block = make(chan struct{})
	h.serve.entered = make(chan struct{}, 1)

	done := make(chan error, 1)

	go func() {
		_, err := h.s.processUp(1, openFrame(7))
		done <- err
	}()

	<-h.serve.entered

	_, err := h.s.processUp(2, openFrame(8))
	require.ErrorIs(t, err, errConcurrent)
	assert.False(t, isClosed(h.s))

	close(h.serve.block)
	require.NoError(t, <-done)
}

func TestSessionDownlinkCreditAndChunking(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	conn := h.serve.conn(t, 0)

	const first = 200 << 10

	_, err = conn.Write(make([]byte, first))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, total := dataFrames(h.s, 7)

		return total == first
	}, time.Second, time.Millisecond)

	frames, _ := dataFrames(h.s, 7)
	require.Len(t, frames, 1, "consecutive DATA must coalesce")
	assert.EqualValues(t, InitialStreamWindow-first, sendCredit(h.s, 7))

	b, i := h.sink.totals()
	assert.EqualValues(t, first+queueItemCost, b[pendingDownlink])
	assert.Equal(t, 1, i[pendingDownlink])

	// Exhaust the credit.
	_, err = conn.Write(make([]byte, InitialStreamWindow-first))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, total := dataFrames(h.s, 7)

		return total == InitialStreamWindow
	}, time.Second, time.Millisecond)

	frames, _ = dataFrames(h.s, 7)
	assert.GreaterOrEqual(t, len(frames), InitialStreamWindow/MaxFramePayload)
	assert.Less(t, len(frames), InitialStreamWindow/DataChunk)

	for _, f := range frames {
		assert.LessOrEqual(t, len(f.Payload), MaxFramePayload)
	}

	assert.Zero(t, sendCredit(h.s, 7))

	written := make(chan error, 1)

	go func() {
		_, err := conn.Write([]byte("z"))
		written <- err
	}()

	select {
	case <-written:
		t.Fatal("write must block without credit")
	case <-time.After(50 * time.Millisecond):
	}

	_, total := dataFrames(h.s, 7)
	assert.Equal(t, InitialStreamWindow, total)

	_, err = h.s.processUp(2, windowFrame(7, 100))
	require.NoError(t, err)

	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("write did not resume after WINDOW")
	}

	require.Eventually(t, func() bool {
		_, total := dataFrames(h.s, 7)

		return total == InitialStreamWindow+1
	}, time.Second, time.Millisecond)
	assert.EqualValues(t, 99, sendCredit(h.s, 7))

	drainDownlink(h.s)

	b, i = h.sink.totals()
	assert.Zero(t, b[pendingDownlink])
	assert.Zero(t, i[pendingDownlink])
}

func TestSessionDownlinkHeadroom(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	h.s.mu.Lock()
	full := h.s.downlinkHeadroomLocked()
	h.s.pendingBytes[pendingDownlink] += full
	assert.Zero(t, h.s.downlinkHeadroomLocked())
	h.s.mu.Unlock()

	conn := h.serve.conn(t, 0)
	written := make(chan error, 1)

	go func() {
		_, err := conn.Write([]byte("z"))
		written <- err
	}()

	select {
	case <-written:
		t.Fatal("write must block without headroom")
	case <-time.After(50 * time.Millisecond):
	}

	h.s.releaseDownlink(pendingDownlink, full, 0)

	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("write did not resume after release")
	}
}

func TestSessionGlobalDownlinkRefusalRetries(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	h.sink.setRefuse(pendingDownlink, true)

	conn := h.serve.conn(t, 0)
	_, err = conn.Write([]byte("abc"))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	_, total := dataFrames(h.s, 7)
	assert.Zero(t, total)

	h.sink.setRefuse(pendingDownlink, false)

	require.Eventually(t, func() bool {
		_, total := dataFrames(h.s, 7)

		return total == 3
	}, 2*time.Second, time.Millisecond)
	assert.EqualValues(t, InitialStreamWindow-3, sendCredit(h.s, 7))
}

func TestSessionWindowCoalesces(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	frames := make([][]byte, 0, 100)
	for range 100 {
		frames = append(frames, dataFrame(7, []byte("ab")))
	}

	_, err = h.s.processUp(2, batch(frames...))
	require.NoError(t, err)

	readN(t, h.serve.conn(t, 0), 200)

	require.Eventually(t, func() bool { return windowSum(h.s, 7) == 200 },
		time.Second, time.Millisecond)

	count := 0

	for _, f := range pendingFrames(h.s) {
		if f.Type == FrameWindow {
			count++
		}
	}

	assert.Equal(t, 1, count)

	b, i := h.sink.totals()
	assert.EqualValues(t, 4+queueItemCost, b[pendingControl])
	assert.Equal(t, 1, i[pendingControl])
}

func TestSessionProxyCloseEmitsClose(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	require.NoError(t, h.serve.conn(t, 0).Close())

	require.Eventually(t, func() bool {
		_, released := h.sink.streams()

		return hasFrame(h.s, FrameClose, 7) && released == 1
	}, time.Second, time.Millisecond)

	count := 0

	for _, f := range pendingFrames(h.s) {
		if f.Type == FrameClose {
			count++
		}
	}

	assert.Equal(t, 1, count)

	// The stream is tombstoned: late client frames are silently accepted.
	_, err = h.s.processUp(2, dataFrame(7, []byte("x")))
	require.NoError(t, err)
}

func TestSessionCloseReleasesEverything(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, batch(openFrame(7), openFrame(8)))
	require.NoError(t, err)

	// Uplink pending: the proxy side of 7 never reads.
	_, err = h.s.processUp(2, batch(dataFrame(7, make([]byte, 1000)), dataFrame(7, []byte("x"))))
	require.NoError(t, err)

	// Downlink pending.
	conn8 := h.serve.conn(t, 1)
	_, err = conn8.Write([]byte("hello"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, total := dataFrames(h.s, 8)

		return total == 5
	}, time.Second, time.Millisecond)

	b, _ := h.sink.totals()
	assert.NotZero(t, b[pendingUplink])
	assert.NotZero(t, b[pendingDownlink])

	h.s.close()
	h.s.close()
	h.s.wait()

	assert.True(t, isClosed(h.s))

	b, i := h.sink.totals()
	assert.Equal(t, [3]int64{}, b)
	assert.Equal(t, [3]int{}, i)

	acquired, released := h.sink.streams()
	assert.Equal(t, 2, acquired)
	assert.Equal(t, 2, released)

	for idx := range 2 {
		conn := h.serve.conn(t, idx)
		_ = conn.SetReadDeadline(time.Now().Add(time.Second)) // fails once the peer is closed
		_, err := conn.Read(make([]byte, 1))
		assert.ErrorIs(t, err, io.EOF)
	}

	assert.False(t, h.s.queueDownlink(Frame{Type: FrameClose, StreamID: 7}, pendingControl))
	assert.Empty(t, pendingFrames(h.s))

	_, err = h.s.processUp(3, openFrame(9))
	require.ErrorIs(t, err, errSessionClosed)
}

func TestSessionTouch(t *testing.T) {
	h := newTestSession(t, nil)

	ts := time.Unix(1_000_000, 0)
	h.s.touch(ts)
	assert.Equal(t, ts, h.s.lastActivity())
}

func TestSessionDownlinkKeepsControlReserve(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, batch(openFrame(7), openFrame(8)))
	require.NoError(t, err)

	limits := h.s.limits
	reserve := limits.controlReserveBytes()

	h.s.mu.Lock()
	// Uplink pending is almost at the data partition limit.
	h.s.pendingBytes[pendingUplink] = limits.MaxPendingPerSession - reserve - 100
	h.s.mu.Unlock()

	t.Cleanup(func() {
		h.s.mu.Lock()
		h.s.pendingBytes[pendingUplink] = 0
		h.s.mu.Unlock()
	})

	// Stream 8 tries to push the whole control reserve downlink.
	conn8 := h.serve.conn(t, 1)

	go conn8.Write(make([]byte, reserve)) //nolint: errcheck

	time.Sleep(50 * time.Millisecond)

	_, total := dataFrames(h.s, 8)
	assert.Zero(t, total, "downlink must not consume the control reserve")

	require.NoError(t, h.serve.conn(t, 0).Close())

	require.Eventually(t, func() bool { return hasFrame(h.s, FrameClose, 7) },
		time.Second, time.Millisecond)
	assert.False(t, isClosed(h.s))
}

func TestSessionClientCloseDropsDownlink(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	conn := h.serve.conn(t, 0)

	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)

	_, err = h.s.processUp(2, dataFrame(7, []byte("ab")))
	require.NoError(t, err)
	readN(t, conn, 2)

	require.Eventually(t, func() bool {
		_, total := dataFrames(h.s, 7)

		return total == 5 && windowSum(h.s, 7) == 2
	}, time.Second, time.Millisecond)

	_, err = h.s.processUp(3, closeFrame(7))
	require.NoError(t, err)

	for _, f := range pendingFrames(h.s) {
		assert.NotEqual(t, uint32(7), f.StreamID, "frame type 0x%02x left for a closed stream", byte(f.Type))
	}

	b, i := h.sink.totals()
	assert.Equal(t, [3]int64{}, b)
	assert.Equal(t, [3]int{}, i)

	h.s.mu.Lock()
	assert.Equal(t, [pendingClasses]int64{}, h.s.pendingBytes)
	assert.Equal(t, [pendingClasses]int{}, h.s.pendingItems)
	h.s.mu.Unlock()

	assert.False(t, isClosed(h.s))
}

func TestSessionProxyCloseKeepsDownlink(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	conn := h.serve.conn(t, 0)

	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool { return hasFrame(h.s, FrameClose, 7) },
		time.Second, time.Millisecond)

	frames := pendingFrames(h.s)
	require.Len(t, frames, 2)
	assert.Equal(t, FrameData, frames[0].Type)
	assert.Equal(t, "hello", string(frames[0].Payload))
	assert.Equal(t, FrameClose, frames[1].Type)
	assert.EqualValues(t, 7, frames[1].StreamID)
}

// --- downlink poll ----------------------------------------------------------

func hasWaiter(s *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.waiter != nil
}

func waitParked(t *testing.T, s *session) {
	t.Helper()

	require.Eventually(t, func() bool { return hasWaiter(s) }, time.Second, time.Millisecond)
}

type pollResult struct {
	body []byte
	next uint64
	err  error
	at   time.Time
}

func goPoll(ctx context.Context, s *session, cursor uint64, longPoll time.Duration) <-chan pollResult {
	ch := make(chan pollResult, 1)

	go func() {
		body, next, err := s.poll(ctx, cursor, longPoll)
		ch <- pollResult{body: body, next: next, err: err, at: time.Now()}
	}()

	return ch
}

func TestSessionPollReturnsBatchAndCursor(t *testing.T) {
	h := newTestSession(t, nil)
	ctx := context.Background()

	require.True(t, h.s.queueDownlink(Frame{Type: FrameWindow, StreamID: 7, Payload: WindowPayload(5)}, pendingControl))
	require.True(t, h.s.queueDownlink(Frame{Type: FrameData, StreamID: 7, Payload: []byte("hello")}, pendingDownlink))

	body, next, err := h.s.poll(ctx, 0, time.Second)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), next)

	frames, err := ParseFrames(body, MaxFramePayload)
	require.NoError(t, err)
	require.Len(t, frames, 2)
	assert.Equal(t, FrameWindow, frames[0].Type)
	assert.Equal(t, uint32(7), frames[0].StreamID)
	n, _ := WindowAmount(frames[0].Payload)
	assert.Equal(t, uint32(5), n)
	assert.Equal(t, FrameData, frames[1].Type)
	assert.Equal(t, []byte("hello"), frames[1].Payload)

	// budget stays charged until the ack
	bs, items := h.sink.totals()
	assert.NotZero(t, bs[pendingControl])
	assert.Equal(t, 1, items[pendingDownlink])

	again, next, err := h.s.poll(ctx, 0, time.Second)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), next)
	assert.Equal(t, body, again)

	start := time.Now()
	body, next, err = h.s.poll(ctx, 1, 10*time.Millisecond)
	require.NoError(t, err)
	assert.Empty(t, body)
	assert.Equal(t, uint64(1), next)
	assert.GreaterOrEqual(t, time.Since(start), 10*time.Millisecond)

	bs, items = h.sink.totals()
	assert.Equal(t, [3]int64{}, bs)
	assert.Equal(t, [3]int{}, items)

	_, _, err = h.s.poll(ctx, 5, time.Second)
	require.ErrorIs(t, err, errCursor)
	require.ErrorIs(t, err, errSessionProtocol)
	assert.True(t, isClosed(h.s))

	_, _, err = h.s.poll(ctx, 1, time.Second)
	require.ErrorIs(t, err, errSessionClosed)
}

func TestSessionPollCursorMismatchWithUnacked(t *testing.T) {
	h := newTestSession(t, nil)
	ctx := context.Background()

	require.True(t, h.s.queueDownlink(Frame{Type: FrameClose, StreamID: 1}, pendingControl))

	_, next, err := h.s.poll(ctx, 0, time.Second)
	require.NoError(t, err)
	require.Equal(t, uint64(1), next)

	_, _, err = h.s.poll(ctx, 2, time.Second)
	require.ErrorIs(t, err, errCursor)
	assert.True(t, isClosed(h.s))

	bs, items := h.sink.totals()
	assert.Equal(t, [3]int64{}, bs)
	assert.Equal(t, [3]int{}, items)
}

func TestSessionPollNewestWins(t *testing.T) {
	h := newTestSession(t, nil)
	ctx := context.Background()

	a := goPoll(ctx, h.s, 0, time.Second)
	waitParked(t, h.s)

	h.s.mu.Lock()
	first := h.s.waiter
	h.s.mu.Unlock()

	b := goPoll(ctx, h.s, 0, time.Second)

	select {
	case r := <-a:
		require.NoError(t, r.err)
		assert.Nil(t, r.body)
		assert.Equal(t, uint64(0), r.next)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("superseded poll did not return")
	}

	require.Eventually(t, func() bool {
		h.s.mu.Lock()
		defer h.s.mu.Unlock()

		return h.s.waiter != nil && h.s.waiter != first
	}, time.Second, time.Millisecond)

	require.True(t, h.s.queueDownlink(Frame{Type: FrameClose, StreamID: 9}, pendingControl))

	select {
	case r := <-b:
		require.NoError(t, r.err)
		assert.Equal(t, uint64(1), r.next)
		frames, err := ParseFrames(r.body, MaxFramePayload)
		require.NoError(t, err)
		require.Len(t, frames, 1)
		assert.Equal(t, FrameClose, frames[0].Type)
		assert.Equal(t, uint32(9), frames[0].StreamID)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("newest poll did not get the frame")
	}
}

func TestSessionPollTimeout(t *testing.T) {
	h := newTestSession(t, nil)

	start := time.Now()
	body, next, err := h.s.poll(context.Background(), 0, 20*time.Millisecond)
	require.NoError(t, err)
	assert.Empty(t, body)
	assert.Equal(t, uint64(0), next)
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	assert.False(t, hasWaiter(h.s))
}

func TestSessionPollCancelled(t *testing.T) {
	h := newTestSession(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	res := goPoll(ctx, h.s, 0, 5*time.Second)
	waitParked(t, h.s)

	cancelled := time.Now()
	cancel()

	select {
	case r := <-res:
		require.ErrorIs(t, r.err, context.Canceled)
		assert.Nil(t, r.body)
		assert.Equal(t, uint64(0), r.next)
		assert.Less(t, r.at.Sub(cancelled), 50*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("cancelled poll did not return")
	}

	assert.False(t, isClosed(h.s))

	require.True(t, h.s.queueDownlink(Frame{Type: FrameClose, StreamID: 3}, pendingControl))

	body, next, err := h.s.poll(context.Background(), 0, time.Second)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), next)
	assert.NotEmpty(t, body)
	assert.False(t, isClosed(h.s))
}

func TestSessionPollSessionClosedWhileParked(t *testing.T) {
	h := newTestSession(t, nil)

	res := goPoll(context.Background(), h.s, 0, 5*time.Second)
	waitParked(t, h.s)
	h.s.close()

	select {
	case r := <-res:
		require.ErrorIs(t, r.err, errSessionClosed)
	case <-time.After(time.Second):
		t.Fatal("parked poll did not notice close")
	}
}

func TestSessionPollTouches(t *testing.T) {
	h := newTestSession(t, nil)
	h.s.touch(time.Unix(1, 0))

	_, _, err := h.s.poll(context.Background(), 0, 0)
	require.NoError(t, err)
	assert.True(t, h.s.lastActivity().After(time.Unix(1, 0)))

	h.s.touch(time.Unix(1, 0))
	_, err = h.s.processUp(1, openFrame(1))
	require.NoError(t, err)
	assert.True(t, h.s.lastActivity().After(time.Unix(1, 0)))
}

func pollFrames(t *testing.T, s *session, cursor uint64) ([]Frame, uint64) {
	t.Helper()

	body, next, err := s.poll(context.Background(), cursor, time.Second)
	require.NoError(t, err)

	frames, err := ParseFrames(body, MaxFramePayload)
	require.NoError(t, err)

	return frames, next
}

func TestSessionBatchLimits(t *testing.T) {
	t.Run("frames", func(t *testing.T) {
		h := newTestSession(t, nil)

		// DATA of distinct streams does not coalesce; this many control
		// items would overflow maxControlItemsPerSession.
		for i := range 5000 {
			require.True(t, h.s.queueDownlink(Frame{
				Type: FrameData, StreamID: uint32(i + 1), Payload: []byte{1},
			}, pendingDownlink))
		}

		frames, next := pollFrames(t, h.s, 0)
		assert.Len(t, frames, MaxBatchFrames)
		assert.Equal(t, uint64(1), next)

		frames, next = pollFrames(t, h.s, 1)
		assert.Len(t, frames, 5000-MaxBatchFrames)
		assert.Equal(t, uint64(2), next)
		assert.Equal(t, uint32(MaxBatchFrames+1), frames[0].StreamID)
	})

	t.Run("bytes", func(t *testing.T) {
		h := newTestSession(t, func(l *Limits) { l.CarrierBatchBytes = 256 << 10 })

		for i := range 3 {
			require.True(t, h.s.queueDownlink(Frame{
				Type: FrameData, StreamID: uint32(i + 1), Payload: make([]byte, 100<<10),
			}, pendingDownlink))
		}

		frames, next := pollFrames(t, h.s, 0)
		assert.Len(t, frames, 2)

		frames, _ = pollFrames(t, h.s, next)
		assert.Len(t, frames, 1)
	})

	t.Run("oversized frame alone", func(t *testing.T) {
		h := newTestSession(t, func(l *Limits) { l.CarrierBatchBytes = 256 << 10 })

		require.True(t, h.s.queueDownlink(Frame{
			Type: FrameData, StreamID: 1, Payload: make([]byte, 1<<20),
		}, pendingDownlink))
		require.True(t, h.s.queueDownlink(Frame{Type: FrameClose, StreamID: 1}, pendingControl))

		frames, next := pollFrames(t, h.s, 0)
		require.Len(t, frames, 1)
		assert.Len(t, frames[0].Payload, 1<<20)

		frames, _ = pollFrames(t, h.s, next)
		require.Len(t, frames, 1)
		assert.Equal(t, FrameClose, frames[0].Type)
	})
}

func TestSessionPollAckWakesDownlinkPump(t *testing.T) {
	h := newTestSession(t, nil)

	_, err := h.s.processUp(1, openFrame(7))
	require.NoError(t, err)

	proxy := h.serve.conn(t, 0)
	_, err = proxy.Write([]byte("abc"))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return hasFrame(h.s, FrameData, 7) }, time.Second, time.Millisecond)

	frames, next := pollFrames(t, h.s, 0)
	require.Len(t, frames, 1)
	assert.Equal(t, []byte("abc"), frames[0].Payload)

	_, _, err = h.s.poll(context.Background(), next, 0)
	require.NoError(t, err)

	bs, items := h.sink.totals()
	assert.Equal(t, [3]int64{}, bs)
	assert.Equal(t, [3]int{}, items)
}
