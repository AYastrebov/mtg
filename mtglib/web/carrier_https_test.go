package web

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCarrier(t *testing.T, mutate func(*Limits)) (*httpsCarrier, *sessionHarness) {
	t.Helper()

	limits := DefaultLimits()
	if mutate != nil {
		mutate(&limits)
	}

	h := newTestSession(t, mutate)

	return newHTTPSCarrier(limits, DefaultTimeouts()), h
}

func upRequest(seq string, contentType string, body []byte) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/up", bytes.NewReader(body))
	if seq != "" {
		r.Header.Set("X-Up-Seq", seq)
	}

	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}

	return r
}

func downRequest(cursor string, body []byte) *http.Request {
	var r *http.Request
	if body == nil {
		r = httptest.NewRequest(http.MethodPost, "/api/v1/down", nil)
	} else {
		r = httptest.NewRequest(http.MethodPost, "/api/v1/down", bytes.NewReader(body))
	}

	r.Header.Set("X-Down-Cursor", cursor)

	return r
}

func TestHTTPSCarrierMode(t *testing.T) {
	c := newHTTPSCarrier(DefaultLimits(), DefaultTimeouts())

	var _ carrier = c

	assert.Equal(t, "https", c.mode())

	h := newTestSession(t, nil)
	rec := httptest.NewRecorder()
	c.handleWS(rec, httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil), h.s)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
}

func TestHandleUpOK(t *testing.T) {
	c, h := newTestCarrier(t, nil)

	var up, down uint64

	c.onBytes = func(u, d uint64) { up += u; down += d }

	body := batch(openFrame(7), dataFrame(7, []byte("abc")))
	rec := httptest.NewRecorder()
	c.handleUp(rec, upRequest("1", "application/octet-stream", body), h.s)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("X-Up-Ack"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.EqualValues(t, len(body), up)
	assert.Zero(t, down)

	conn := h.serve.conn(t, 0)
	assert.Equal(t, "abc", string(readN(t, conn, 3)))
}

func TestHandleUpRejections(t *testing.T) {
	ok := openFrame(7)

	tests := []struct {
		name string
		req  func() *http.Request
	}{
		{"missing seq", func() *http.Request { return upRequest("", "application/octet-stream", ok) }},
		{"leading zero seq", func() *http.Request { return upRequest("01", "application/octet-stream", ok) }},
		{"signed seq", func() *http.Request { return upRequest("+1", "application/octet-stream", ok) }},
		{"wrong content type", func() *http.Request { return upRequest("1", "text/plain", ok) }},
		{"content type with params", func() *http.Request {
			return upRequest("1", "application/octet-stream; charset=binary", ok)
		}},
		{"missing content type", func() *http.Request { return upRequest("1", "", ok) }},
		{"empty body", func() *http.Request { return upRequest("1", "application/octet-stream", nil) }},
		{"oversized body", func() *http.Request {
			return upRequest("1", "application/octet-stream", make([]byte, DefaultLimits().MaxBodyBytes+1))
		}},
		{"garbage batch", func() *http.Request { return upRequest("1", "application/octet-stream", []byte{1, 2}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, h := newTestCarrier(t, nil)
			c.onBytes = func(_, _ uint64) { t.Error("onBytes must not be called") }

			rec := httptest.NewRecorder()
			c.handleUp(rec, tt.req(), h.s)

			assert.Equal(t, http.StatusNotFound, rec.Code)
			assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			assert.Empty(t, rec.Header().Get("X-Up-Ack"))
			assert.Zero(t, rec.Body.Len())
		})
	}
}

func TestHandleUp503OnBackpressure(t *testing.T) {
	c, h := newTestCarrier(t, nil)

	rec := httptest.NewRecorder()
	c.handleUp(rec, upRequest("1", "application/octet-stream", openFrame(7)), h.s)
	require.Equal(t, http.StatusNoContent, rec.Code)

	h.sink.setRefuse(pendingUplink, true)

	body := dataFrame(7, []byte("abc"))
	rec = httptest.NewRecorder()
	c.handleUp(rec, upRequest("2", "application/octet-stream", body), h.s)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.False(t, isClosed(h.s))

	h.sink.setRefuse(pendingUplink, false)

	rec = httptest.NewRecorder()
	c.handleUp(rec, upRequest("2", "application/octet-stream", body), h.s)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "2", rec.Header().Get("X-Up-Ack"))
}

func TestHandleUpProtocolErrorIs404(t *testing.T) {
	c, h := newTestCarrier(t, nil)

	rec := httptest.NewRecorder()
	c.handleUp(rec, upRequest("5", "application/octet-stream", openFrame(7)), h.s)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.True(t, isClosed(h.s))

	rec = httptest.NewRecorder()
	c.handleUp(rec, upRequest("1", "application/octet-stream", openFrame(7)), h.s)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleDownBatch(t *testing.T) {
	c, h := newTestCarrier(t, nil)

	var up, down uint64

	c.onBytes = func(u, d uint64) { up += u; down += d }

	require.True(t, h.s.queueDownlink(Frame{Type: FrameData, StreamID: 7, Payload: []byte("hello")}, pendingDownlink))

	rec := httptest.NewRecorder()
	c.handleDown(rec, downRequest("0", nil), h.s)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	assert.Equal(t, "1", rec.Header().Get("X-Down-Cursor"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))

	want := dataFrame(7, []byte("hello"))
	assert.Equal(t, strconv.Itoa(len(want)), rec.Header().Get("Content-Length"))
	assert.Equal(t, want, rec.Body.Bytes())
	assert.Zero(t, up)
	assert.EqualValues(t, len(want), down)
}

func TestHandleDownEmpty(t *testing.T) {
	limits := DefaultLimits()
	timeouts := DefaultTimeouts()
	timeouts.LongPoll = 10 * time.Millisecond
	c := newHTTPSCarrier(limits, timeouts)
	h := newTestSession(t, nil)

	rec := httptest.NewRecorder()
	c.handleDown(rec, downRequest("0", nil), h.s)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "0", rec.Header().Get("X-Down-Cursor"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.Zero(t, rec.Body.Len())
}

func TestHandleDownRejectsBody(t *testing.T) {
	tests := []struct {
		name string
		req  func() *http.Request
	}{
		{"body", func() *http.Request { return downRequest("0", []byte{1}) }},
		{"content type", func() *http.Request {
			r := downRequest("0", nil)
			r.Header.Set("Content-Type", "application/octet-stream")

			return r
		}},
		{"missing cursor", func() *http.Request {
			r := downRequest("0", nil)
			r.Header.Del("X-Down-Cursor")

			return r
		}},
		{"non-canonical cursor", func() *http.Request { return downRequest("00", nil) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, h := newTestCarrier(t, nil)

			rec := httptest.NewRecorder()
			c.handleDown(rec, tt.req(), h.s)

			assert.Equal(t, http.StatusNotFound, rec.Code)
			assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			assert.Zero(t, rec.Body.Len())
		})
	}
}

func TestHandleDownBadCursorIs404(t *testing.T) {
	c, h := newTestCarrier(t, nil)

	rec := httptest.NewRecorder()
	c.handleDown(rec, downRequest("5", nil), h.s)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.True(t, isClosed(h.s))
}

// writeSpy records whether a handler touched the response at all.
type writeSpy struct {
	header  http.Header
	touched bool
}

func (w *writeSpy) Header() http.Header {
	return w.header
}

func (w *writeSpy) Write(p []byte) (int, error) {
	w.touched = true

	return len(p), nil
}

func (w *writeSpy) WriteHeader(int) {
	w.touched = true
}

func TestHandleDownClientGone(t *testing.T) {
	c, h := newTestCarrier(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	req := downRequest("0", nil).WithContext(ctx)
	spy := &writeSpy{header: http.Header{}}
	done := make(chan struct{})

	go func() {
		defer close(done)
		c.handleDown(spy, req, h.s)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("handler did not return after the client went away")
	}

	assert.False(t, spy.touched)
	assert.Empty(t, spy.header)
	assert.False(t, isClosed(h.s))

	// the next poll with the same cursor still works
	require.True(t, h.s.queueDownlink(Frame{Type: FrameData, StreamID: 7, Payload: []byte("x")}, pendingDownlink))

	rec := httptest.NewRecorder()
	c.handleDown(rec, downRequest("0", nil), h.s)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("X-Down-Cursor"))
	assert.Equal(t, dataFrame(7, []byte("x")), rec.Body.Bytes())

	// and a lost response is replayed verbatim
	rec2 := httptest.NewRecorder()
	c.handleDown(rec2, downRequest("0", nil), h.s)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, rec.Body.Bytes(), rec2.Body.Bytes())
	assert.Equal(t, "1", rec2.Header().Get("X-Down-Cursor"))
}
