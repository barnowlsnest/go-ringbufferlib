package ringbuffer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-ringbufferlib/pkg/ringbuffer"
)

type RingBufferSuite struct {
	suite.Suite
}

func TestRingBufferSuite(t *testing.T) {
	suite.Run(t, new(RingBufferSuite))
}

type op int

const (
	opWrite op = iota
	opRead
	opReadByte
	opReadRune
	opReset
	opTruncate
	opClose
)

type step struct {
	op      op
	data    string
	readLen int
	want    string
	wantN   int
	keep    int
	wantErr error
}

func do(rb *ringbuffer.RingBuffer, st *step) (n int, read string, err error) {
	switch st.op {
	case opWrite:
		p := []byte(st.data) // exercises Write itself; WriteString has its own tests
		n, err = rb.Write(p)
	case opRead:
		buf := make([]byte, st.readLen)
		n, err = rb.Read(buf)
		read = string(buf[:n])
	case opReadByte:
		var b byte
		b, err = rb.ReadByte()
		if err == nil {
			n, read = 1, string(b)
		}
	case opReadRune:
		var r rune
		r, n, err = rb.ReadRune()
		if err == nil {
			read = string(r)
		}
	case opReset:
		rb.Reset()
	case opTruncate:
		err = rb.Truncate(st.keep)
	default:
		err = rb.Close()
	}
	return n, read, err
}

func (s *RingBufferSuite) TestSequential() {
	tests := []struct {
		name  string
		size  uint64
		opts  []ringbuffer.Option
		steps []step
	}{
		{
			name: "write then read",
			size: 8,
			steps: []step{
				{op: opWrite, data: "msg", wantN: 3},
				{op: opRead, readLen: 8, want: "msg"},
				{op: opRead, readLen: 1, wantErr: ringbuffer.ErrEmpty},
			},
		},
		{
			name: "read from empty buffer",
			size: 8,
			steps: []step{
				{op: opRead, readLen: 4, wantErr: ringbuffer.ErrEmpty},
			},
		},
		{
			name: "zero-length write and read",
			size: 8,
			steps: []step{
				{op: opWrite, data: "", wantN: 0},
				{op: opRead, readLen: 0, want: ""},
			},
		},
		{
			name: "destination larger than capacity",
			size: 4,
			steps: []step{
				{op: opWrite, data: "ok", wantN: 2},
				{op: opRead, readLen: 512, want: "ok"},
			},
		},
		{
			name: "partial reads keep order",
			size: 8,
			steps: []step{
				{op: opWrite, data: "p1p2p3", wantN: 6},
				{op: opRead, readLen: 2, want: "p1"},
				{op: opRead, readLen: 2, want: "p2"},
				{op: opRead, readLen: 8, want: "p3"},
			},
		},
		{
			name: "wraparound without overwrite",
			size: 4,
			opts: []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			steps: []step{
				{op: opWrite, data: "old", wantN: 3},
				{op: opRead, readLen: 2, want: "ol"},
				{op: opWrite, data: "new", wantN: 3},
				{op: opRead, readLen: 4, want: "dnew"},
			},
		},
		{
			name: "full cycle reuses buffer",
			size: 4,
			opts: []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			steps: []step{
				{op: opWrite, data: "full", wantN: 4},
				{op: opRead, readLen: 4, want: "full"},
				{op: opWrite, data: "next", wantN: 4},
				{op: opRead, readLen: 4, want: "next"},
			},
		},
		{
			name: "overwrite drops oldest",
			size: 4,
			steps: []step{
				{op: opWrite, data: "--ke", wantN: 4},
				{op: opWrite, data: "ep", wantN: 2},
				{op: opRead, readLen: 4, want: "keep"},
			},
		},
		{
			name: "overwrite when exactly full",
			size: 4,
			steps: []step{
				{op: opWrite, data: "-kee", wantN: 4},
				{op: opWrite, data: "p", wantN: 1},
				{op: opRead, readLen: 4, want: "keep"},
			},
		},
		{
			name: "overwrite across wrap",
			size: 4,
			steps: []step{
				{op: opWrite, data: "---", wantN: 3},
				{op: opRead, readLen: 2, want: "--"},
				{op: opWrite, data: "-ke", wantN: 3},
				{op: opWrite, data: "ep", wantN: 2},
				{op: opRead, readLen: 4, want: "keep"},
			},
		},
		{
			name: "oversize write rejected in overwrite mode",
			size: 4,
			steps: []step{
				{op: opWrite, data: "too-long", wantN: 0, wantErr: ringbuffer.ErrTooLargeForWrite},
				{op: opRead, readLen: 1, wantErr: ringbuffer.ErrEmpty},
			},
		},
		{
			name: "size rounds up to power of two",
			size: 6,
			steps: []step{
				{op: opWrite, data: "rounded8", wantN: 8},
				{op: opRead, readLen: 8, want: "rounded8"},
			},
		},
		{
			name: "size zero uses default",
			size: 0,
			steps: []step{
				{op: opWrite, data: "d", wantN: 1},
				{op: opRead, readLen: 1, want: "d"},
			},
		},
		{
			name: "read byte in order",
			size: 8,
			steps: []step{
				{op: opWrite, data: "go", wantN: 2},
				{op: opReadByte, want: "g"},
				{op: opReadByte, want: "o"},
				{op: opReadByte, wantErr: ringbuffer.ErrEmpty},
			},
		},
		{
			name: "read byte across wrap",
			size: 4,
			steps: []step{
				{op: opWrite, data: "---", wantN: 3},
				{op: opRead, readLen: 3, want: "---"},
				{op: opWrite, data: "wrap", wantN: 4},
				{op: opReadByte, want: "w"},
				{op: opReadByte, want: "r"},
				{op: opRead, readLen: 4, want: "ap"},
			},
		},
		{
			name: "read byte after close returns rest then EOF",
			size: 8,
			steps: []step{
				{op: opWrite, data: "last", wantN: 4},
				{op: opClose},
				{op: opRead, readLen: 3, want: "las"},
				{op: opReadByte, want: "t"},
				{op: opReadByte, wantErr: io.EOF},
			},
		},
		{
			name: "reset discards unread bytes",
			size: 8,
			steps: []step{
				{op: opWrite, data: "stale", wantN: 5},
				{op: opReset},
				{op: opRead, readLen: 8, wantErr: ringbuffer.ErrEmpty},
				{op: opWrite, data: "fresh", wantN: 5},
				{op: opRead, readLen: 8, want: "fresh"},
			},
		},
		{
			name: "reset after close keeps buffer closed",
			size: 8,
			steps: []step{
				{op: opWrite, data: "stale", wantN: 5},
				{op: opClose},
				{op: opReset},
				{op: opRead, readLen: 8, wantErr: io.EOF},
				{op: opWrite, data: "late", wantErr: io.EOF},
			},
		},
		{
			name: "closed buffer returns rest then EOF",
			size: 8,
			steps: []step{
				{op: opWrite, data: "rest", wantN: 4},
				{op: opClose},
				{op: opRead, readLen: 2, want: "re"},
				{op: opRead, readLen: 4, want: "st"},
				{op: opRead, readLen: 2, wantErr: io.EOF},
				{op: opClose},
			},
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size, tt.opts...)
			for i, st := range tt.steps {
				n, got, err := do(rb, &st)
				s.Require().ErrorIs(err, st.wantErr, "step %d", i)
				switch st.op {
				case opWrite:
					s.Require().Equal(st.wantN, n, "step %d", i)
				case opRead, opReadByte:
					s.Require().Equal(st.want, got, "step %d", i)
				}
			}
		})
	}
}

// synctest.Wait returns once every other goroutine is durably blocked
// (sync.Cond.Wait counts), so "does it block?" needs no sleeps.
func (s *RingBufferSuite) TestBlocking() {
	tests := []struct {
		name    string
		opts    []ringbuffer.Option
		prefill string
		blocked step
		release step
		wantN   int
		wantErr error
	}{
		{
			name:    "read waits for write",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			blocked: step{op: opRead, readLen: 4},
			release: step{op: opWrite, data: "go"},
			wantN:   2,
		},
		{
			name:    "read byte waits for write",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			blocked: step{op: opReadByte},
			release: step{op: opWrite, data: "go"},
			wantN:   1,
		},
		{
			name:    "read rune waits for write",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			blocked: step{op: opReadRune},
			release: step{op: opWrite, data: "é"},
			wantN:   2,
		},
		{
			name:    "read rune waits for rest of partial rune",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			prefill: "\xe2\x82",
			blocked: step{op: opReadRune},
			release: step{op: opWrite, data: "\xac"},
			wantN:   3,
		},
		{
			name:    "read rune on partial rune released by close",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			prefill: "\xe2\x82",
			blocked: step{op: opReadRune},
			release: step{op: opClose},
			wantN:   1,
		},
		{
			name:    "read released by close",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			blocked: step{op: opRead, readLen: 4},
			release: step{op: opClose},
			wantN:   0,
			wantErr: io.EOF,
		},
		{
			name:    "write waits for room",
			opts:    []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			prefill: "full",
			blocked: step{op: opWrite, data: "more"},
			release: step{op: opRead, readLen: 4},
			wantN:   4,
		},
		{
			name:    "write released by reset",
			opts:    []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			prefill: "full",
			blocked: step{op: opWrite, data: "more"},
			release: step{op: opReset},
			wantN:   4,
		},
		{
			name:    "write released by truncate",
			opts:    []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			prefill: "full",
			blocked: step{op: opWrite, data: "more"},
			release: step{op: opTruncate, keep: 0},
			wantN:   4,
		},
		{
			name:    "write released by close",
			opts:    []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			prefill: "full",
			blocked: step{op: opWrite, data: "more"},
			release: step{op: opClose},
			wantN:   0,
			wantErr: io.EOF,
		},
		{
			name: "write released by drain",
			opts: []ringbuffer.Option{
				ringbuffer.WithOverWrite(false),
				ringbuffer.WithDrainOnClose(true, time.Second),
			},
			prefill: "full",
			blocked: step{op: opWrite, data: "more"},
			release: step{op: opClose},
			wantN:   0,
			wantErr: ringbuffer.ErrDraining,
		},
	}
	for i := range tests {
		tt := &tests[i]
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				rb := ringbuffer.New(4, tt.opts...)
				if tt.prefill != "" {
					_, err := rb.WriteString(tt.prefill)
					require.NoError(t, err)
				}

				type result struct {
					n   int
					err error
				}
				done := make(chan result, 1)
				go func() {
					n, _, err := do(rb, &tt.blocked)
					done <- result{n: n, err: err}
				}()

				synctest.Wait()
				require.Empty(t, done, "operation should block")

				_, _, _ = do(rb, &tt.release)
				synctest.Wait()
				require.Len(t, done, 1, "operation should be released")

				res := <-done
				require.ErrorIs(t, res.err, tt.wantErr)
				require.Equal(t, tt.wantN, res.n)
			})
		})
	}
}

// Fake clock: the drain timeout elapses instantly and exactly.
func (s *RingBufferSuite) TestDrainOnClose() {
	const timeout = 5 * time.Second
	tests := []struct {
		name        string
		readDuring  bool
		wantElapsed time.Duration
		wantLeft    string
	}{
		{
			name:        "finishes when reader empties buffer",
			readDuring:  true,
			wantElapsed: 0,
			wantLeft:    "",
		},
		{
			name:        "gives up after timeout",
			readDuring:  false,
			wantElapsed: timeout,
			wantLeft:    "left",
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				rb := ringbuffer.New(8, ringbuffer.WithDrainOnClose(true, timeout))
				_, err := rb.WriteString("left")
				require.NoError(t, err)

				start := time.Now()
				closed := make(chan struct{})
				go func() {
					_ = rb.Close()
					close(closed)
				}()
				synctest.Wait()

				_, err = rb.WriteString("late")
				require.ErrorIs(t, err, ringbuffer.ErrDraining, "writes rejected while draining")

				if tt.readDuring {
					buf := make([]byte, 8)
					n, err := rb.Read(buf)
					require.NoError(t, err, "reads allowed while draining")
					require.Equal(t, "left", string(buf[:n]))
				}
				<-closed
				require.Equal(t, tt.wantElapsed, time.Since(start))

				left, err := io.ReadAll(rb)
				require.NoError(t, err)
				require.Equal(t, tt.wantLeft, string(left))
				require.NoError(t, rb.Close(), "Close is idempotent")
			})
		})
	}
}

func (s *RingBufferSuite) TestReadRuneOnBufferSmallerThanRuneDoesNotDeadlock() {
	synctest.Test(s.T(), func(t *testing.T) {
		rb := ringbuffer.New(2,
			ringbuffer.WithOverWrite(false),
			ringbuffer.WithReadWaitOnEmpty(true),
		)
		const owl = "🦉"
		type result struct {
			n   int
			err error
		}
		written := make(chan result, 1)
		go func() {
			n, err := rb.WriteString(owl)
			written <- result{n: n, err: err}
		}()
		for i := range len(owl) {
			r, size, err := rb.ReadRune()
			require.NoError(t, err, "rune %d", i)
			require.Equal(t, utf8.RuneError, r, "rune %d", i)
			require.Equal(t, 1, size, "rune %d", i)
		}
		res := <-written
		require.NoError(t, res.err)
		require.Equal(t, len(owl), res.n)
	})
}

func (s *RingBufferSuite) TestReadRuneWakesWhenDrainingStarts() {
	synctest.Test(s.T(), func(t *testing.T) {
		const drainTimeout = 5 * time.Second
		rb := ringbuffer.New(8,
			ringbuffer.WithReadWaitOnEmpty(true),
			ringbuffer.WithDrainOnClose(true, drainTimeout),
		)
		_, err := rb.WriteString("\xe2\x82")
		require.NoError(t, err)

		type result struct {
			r    rune
			size int
			err  error
		}
		got := make(chan result, 1)
		go func() {
			r, size, err := rb.ReadRune()
			got <- result{r: r, size: size, err: err}
		}()
		synctest.Wait()

		start := time.Now()
		closed := make(chan struct{})
		go func() {
			_ = rb.Close()
			close(closed)
		}()
		res := <-got
		require.Zero(t, time.Since(start), "reader wakes as soon as draining starts")
		require.NoError(t, res.err)
		require.Equal(t, utf8.RuneError, res.r)
		require.Equal(t, 1, res.size)
		require.Equal(t, "\x82", rb.String())
		<-closed
	})
}

func (s *RingBufferSuite) TestTimeoutReleasesBlockedCall() {
	const timeout = 3 * time.Second
	readTimeout := []ringbuffer.Option{
		ringbuffer.WithReadWaitOnEmpty(true),
		ringbuffer.WithReadTimeout(timeout),
	}
	writeTimeout := []ringbuffer.Option{
		ringbuffer.WithOverWrite(false),
		ringbuffer.WithWriteTimeout(timeout),
	}
	tests := []struct {
		name     string
		opts     []ringbuffer.Option
		prefill  string
		blocked  step
		wantN    int
		wantLeft string
	}{
		{
			name:    "read on empty buffer",
			opts:    readTimeout,
			blocked: step{op: opRead, readLen: 4},
			wantN:   0,
		},
		{
			name:    "read byte on empty buffer",
			opts:    readTimeout,
			blocked: step{op: opReadByte},
			wantN:   0,
		},
		{
			name:    "read rune on empty buffer",
			opts:    readTimeout,
			blocked: step{op: opReadRune},
			wantN:   0,
		},
		{
			name:     "read rune on partial rune",
			opts:     readTimeout,
			prefill:  "\xe2\x82",
			blocked:  step{op: opReadRune},
			wantN:    0,
			wantLeft: "\xe2\x82",
		},
		{
			name:     "write into full buffer",
			opts:     writeTimeout,
			prefill:  "full",
			blocked:  step{op: opWrite, data: "more"},
			wantN:    0,
			wantLeft: "full",
		},
		{
			name:     "write reports bytes written before timeout",
			opts:     writeTimeout,
			prefill:  "ful",
			blocked:  step{op: opWrite, data: "more"},
			wantN:    1,
			wantLeft: "fulm",
		},
	}
	for i := range tests {
		tt := &tests[i]
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				rb := ringbuffer.New(4, tt.opts...)
				if tt.prefill != "" {
					_, err := rb.WriteString(tt.prefill)
					require.NoError(t, err)
				}
				start := time.Now()

				n, _, err := do(rb, &tt.blocked)
				require.ErrorIs(t, err, ringbuffer.ErrTimeout)
				require.Equal(t, tt.wantN, n)
				require.Equal(t, timeout, time.Since(start))
				require.Equal(t, tt.wantLeft, rb.String(), "unread bytes kept")
			})
		})
	}
}

func (s *RingBufferSuite) TestNonPositiveTimeoutWaitsForever() {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{
			name:    "zero",
			timeout: 0,
		},
		{
			name:    "negative",
			timeout: -time.Second,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				rb := ringbuffer.New(4,
					ringbuffer.WithReadWaitOnEmpty(true),
					ringbuffer.WithReadTimeout(tt.timeout),
				)
				done := make(chan error, 1)
				go func() {
					_, err := rb.ReadByte()
					done <- err
				}()
				time.Sleep(time.Hour)
				synctest.Wait()
				require.Empty(t, done, "read should still block")

				require.NoError(t, rb.WriteByte('!'))
				require.NoError(t, <-done)
			})
		})
	}
}

// Each wait shorter than the timeout keeps the call alive even when the whole
// call takes longer.
func (s *RingBufferSuite) TestWriteTimeoutRestartsOnProgress() {
	synctest.Test(s.T(), func(t *testing.T) {
		const (
			timeout   = 2 * time.Second
			readEvery = 1500 * time.Millisecond
			payload   = "keeps-moving"
		)
		rb := ringbuffer.New(4,
			ringbuffer.WithOverWrite(false),
			ringbuffer.WithWriteTimeout(timeout),
		)
		var drained bytes.Buffer
		readerDone := make(chan error, 1)
		go func() {
			chunk := make([]byte, 4)
			for drained.Len() < len(payload) {
				time.Sleep(readEvery)
				n, err := rb.Read(chunk)
				if err != nil {
					readerDone <- err
					return
				}
				drained.Write(chunk[:n])
			}
			readerDone <- nil
		}()

		start := time.Now()
		n, err := rb.WriteString(payload)
		require.NoError(t, err)
		require.Equal(t, len(payload), n)
		require.Greater(t, time.Since(start), timeout, "whole write outlasts the timeout")
		require.NoError(t, <-readerDone)
		require.Equal(t, payload, drained.String())
	})
}

func (s *RingBufferSuite) TestReadTimeoutAllowsSlowContinuousStream() {
	synctest.Test(s.T(), func(t *testing.T) {
		const (
			timeout    = 2 * time.Second
			writeEvery = 1500 * time.Millisecond
		)
		chunks := []string{"first;", "second;", "third;"}
		rb := ringbuffer.New(64,
			ringbuffer.WithReadWaitOnEmpty(true),
			ringbuffer.WithReadTimeout(timeout),
		)
		produced := make(chan error, 1)
		go func() {
			for _, c := range chunks {
				time.Sleep(writeEvery)
				if _, err := rb.WriteString(c); err != nil {
					produced <- err
					return
				}
			}
			produced <- rb.Close()
		}()

		start := time.Now()
		var got bytes.Buffer
		_, err := io.Copy(&got, rb)
		require.NoError(t, err)
		require.NoError(t, <-produced)
		require.Equal(t, "first;second;third;", got.String())
		require.Equal(t, time.Duration(len(chunks))*writeEvery, time.Since(start))
	})
}

func (s *RingBufferSuite) TestTimeoutIgnoredByCallsThatDoNotBlock() {
	synctest.Test(s.T(), func(t *testing.T) {
		rb := ringbuffer.New(4,
			ringbuffer.WithReadTimeout(time.Nanosecond),
			ringbuffer.WithWriteTimeout(time.Nanosecond),
		)
		_, err := rb.ReadByte()
		require.ErrorIs(t, err, ringbuffer.ErrEmpty, "non-waiting read still reports empty")

		_, err = rb.WriteString("over")
		require.NoError(t, err)
		_, err = rb.WriteString("full")
		require.NoError(t, err, "overwrite-mode write never waits")
		require.Equal(t, "full", rb.String())
	})
}

var errStreamAborted = errors.New("stream aborted by caller")

func (s *RingBufferSuite) TestContextCancelReleasesBlockedCall() {
	waitOnEmpty := []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)}
	backpressure := []ringbuffer.Option{ringbuffer.WithOverWrite(false)}
	tests := []struct {
		name    string
		opts    []ringbuffer.Option
		prefill string
		blocked step
		wantN   int
	}{
		{
			name:    "read on empty buffer",
			opts:    waitOnEmpty,
			blocked: step{op: opRead, readLen: 4},
			wantN:   0,
		},
		{
			name:    "read byte on empty buffer",
			opts:    waitOnEmpty,
			blocked: step{op: opReadByte},
			wantN:   0,
		},
		{
			name:    "read rune on empty buffer",
			opts:    waitOnEmpty,
			blocked: step{op: opReadRune},
			wantN:   0,
		},
		{
			name:    "read rune on partial rune",
			opts:    waitOnEmpty,
			prefill: "\xe2\x82",
			blocked: step{op: opReadRune},
			wantN:   0,
		},
		{
			name:    "write into full buffer",
			opts:    backpressure,
			prefill: "full",
			blocked: step{op: opWrite, data: "more"},
			wantN:   0,
		},
		{
			name:    "write reports bytes written before cancel",
			opts:    backpressure,
			prefill: "ful",
			blocked: step{op: opWrite, data: "more"},
			wantN:   1,
		},
	}
	for i := range tests {
		tt := &tests[i]
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(t.Context())
				opts := append([]ringbuffer.Option{ringbuffer.WithContext(ctx)}, tt.opts...)
				rb := ringbuffer.New(4, opts...)
				if tt.prefill != "" {
					_, err := rb.WriteString(tt.prefill)
					require.NoError(t, err)
				}

				type result struct {
					n   int
					err error
				}
				done := make(chan result, 1)
				go func() {
					n, _, err := do(rb, &tt.blocked)
					done <- result{n: n, err: err}
				}()
				synctest.Wait()
				require.Empty(t, done, "operation should block")

				start := time.Now()
				cancel(errStreamAborted)
				res := <-done
				require.Zero(t, time.Since(start), "released as soon as ctx is canceled")
				require.ErrorIs(t, res.err, errStreamAborted)
				require.Equal(t, tt.wantN, res.n)
			})
		})
	}
}

// Unlike Close, cancel discards unread bytes: every later call fails.
func (s *RingBufferSuite) TestCanceledContextFailsLaterCalls() {
	tests := []struct {
		name string
		call step
	}{
		{
			name: "read with unread bytes",
			call: step{op: opRead, readLen: 4},
		},
		{
			name: "read byte with unread bytes",
			call: step{op: opReadByte},
		},
		{
			name: "read rune with unread bytes",
			call: step{op: opReadRune},
		},
		{
			name: "write",
			call: step{op: opWrite, data: "more"},
		},
	}
	for i := range tests {
		tt := &tests[i]
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(t.Context())
				rb := ringbuffer.New(8, ringbuffer.WithContext(ctx))
				_, err := rb.WriteString("unread")
				require.NoError(t, err)
				cancel(errStreamAborted)
				synctest.Wait()

				n, _, err := do(rb, &tt.call)
				require.ErrorIs(t, err, errStreamAborted)
				require.Zero(t, n)
			})
		})
	}
}

func (s *RingBufferSuite) TestCancelWithoutCauseReturnsContextCanceled() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		rb := ringbuffer.New(4, ringbuffer.WithContext(ctx))
		cancel()
		synctest.Wait()

		_, err := rb.WriteString("late")
		require.ErrorIs(t, err, context.Canceled)
	})
}

func (s *RingBufferSuite) TestCancelImmediatelyFailsBeforeAbortCallback() {
	ctx, cancel := context.WithCancelCause(s.T().Context())
	rb := ringbuffer.New(8, ringbuffer.WithContext(ctx))
	_, err := rb.WriteString("unread")
	s.Require().NoError(err)

	cancel(errStreamAborted)
	_, err = rb.ReadByte()
	s.Require().ErrorIs(err, errStreamAborted)
	s.Require().Equal("unread", rb.String())
}

func (s *RingBufferSuite) TestCancelImmediatelyBeforeCloseAbortsBuffer() {
	ctx, cancel := context.WithCancelCause(s.T().Context())
	rb := ringbuffer.New(8, ringbuffer.WithContext(ctx))
	_, err := rb.WriteString("unread")
	s.Require().NoError(err)

	cancel(errStreamAborted)
	s.Require().NoError(rb.Close())
	_, err = rb.ReadByte()
	s.Require().ErrorIs(err, errStreamAborted)
	s.Require().Equal("unread", rb.String())
}

func (s *RingBufferSuite) TestAlreadyCanceledContextAbortsAtNew() {
	ctx, cancel := context.WithCancelCause(s.T().Context())
	cancel(errStreamAborted)

	rb := ringbuffer.New(4, ringbuffer.WithContext(ctx))
	_, err := rb.WriteString("first")
	s.Require().ErrorIs(err, errStreamAborted, "no call slips through before the abort")
}

func (s *RingBufferSuite) TestContextCancelStopsDrainOnClose() {
	synctest.Test(s.T(), func(t *testing.T) {
		const drainTimeout = 5 * time.Second
		ctx, cancel := context.WithCancelCause(t.Context())
		rb := ringbuffer.New(8,
			ringbuffer.WithContext(ctx),
			ringbuffer.WithDrainOnClose(true, drainTimeout),
		)
		_, err := rb.WriteString("unread")
		require.NoError(t, err)

		closed := make(chan error, 1)
		go func() {
			closed <- rb.Close()
		}()
		synctest.Wait()
		require.Empty(t, closed, "close should be draining")

		start := time.Now()
		cancel(errStreamAborted)
		require.NoError(t, <-closed)
		require.Zero(t, time.Since(start), "drain stops as soon as ctx is canceled")

		_, err = rb.ReadByte()
		require.ErrorIs(t, err, errStreamAborted)
	})
}

func (s *RingBufferSuite) TestCancelAfterCloseKeepsLeftoverReadable() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		rb := ringbuffer.New(8, ringbuffer.WithContext(ctx))
		const leftover = "leftover"
		_, err := rb.WriteString(leftover)
		require.NoError(t, err)
		require.NoError(t, rb.Close())
		cancel(errStreamAborted)
		synctest.Wait()

		got, err := io.ReadAll(rb)
		require.NoError(t, err, "a closed buffer ends with io.EOF, not the cause")
		require.Equal(t, leftover, string(got))
	})
}

func (s *RingBufferSuite) TestLastWithContextWins() {
	synctest.Test(s.T(), func(t *testing.T) {
		replaced, cancelReplaced := context.WithCancel(t.Context())
		defer cancelReplaced()
		effective, cancelEffective := context.WithCancelCause(t.Context())
		rb := ringbuffer.New(4,
			ringbuffer.WithContext(replaced),
			ringbuffer.WithContext(effective),
		)

		cancelReplaced()
		synctest.Wait()
		_, err := rb.WriteString("kept")
		require.NoError(t, err, "replaced context has no effect")

		cancelEffective(errStreamAborted)
		synctest.Wait()
		_, err = rb.WriteString("late")
		require.ErrorIs(t, err, errStreamAborted)
	})
}

func (s *RingBufferSuite) TestNilContextIsIgnored() {
	var missing context.Context
	rb := ringbuffer.New(4, ringbuffer.WithContext(missing))

	const payload = "safe"
	_, err := rb.WriteString(payload)
	s.Require().NoError(err)
	s.Require().Equal(payload, rb.String())
}

var (
	_ io.ByteReader   = (*ringbuffer.RingBuffer)(nil)
	_ io.RuneReader   = (*ringbuffer.RingBuffer)(nil)
	_ io.ByteWriter   = (*ringbuffer.RingBuffer)(nil)
	_ io.StringWriter = (*ringbuffer.RingBuffer)(nil)
	_ fmt.Stringer    = (*ringbuffer.RingBuffer)(nil)
)

func (s *RingBufferSuite) TestSizeAccessors() {
	tests := []struct {
		name          string
		size          uint64
		written       string
		readLen       int
		wantLen       int
		wantCap       int
		wantAvailable int
	}{
		{
			name:          "empty buffer",
			size:          8,
			wantLen:       0,
			wantCap:       8,
			wantAvailable: 8,
		},
		{
			name:          "partly filled",
			size:          8,
			written:       "half",
			wantLen:       4,
			wantCap:       8,
			wantAvailable: 4,
		},
		{
			name:          "full",
			size:          4,
			written:       "full",
			wantLen:       4,
			wantCap:       4,
			wantAvailable: 0,
		},
		{
			name:          "read frees space",
			size:          8,
			written:       "consumed",
			readLen:       3,
			wantLen:       5,
			wantCap:       8,
			wantAvailable: 3,
		},
		{
			name:          "capacity rounded up to power of two",
			size:          6,
			wantLen:       0,
			wantCap:       8,
			wantAvailable: 8,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size)
			_, err := rb.WriteString(tt.written)
			s.Require().NoError(err)
			if tt.readLen > 0 {
				_, err = rb.Read(make([]byte, tt.readLen))
				s.Require().NoError(err)
			}
			s.Require().Equal(tt.wantLen, rb.Len())
			s.Require().Equal(tt.wantCap, rb.Cap())
			s.Require().Equal(tt.wantAvailable, rb.Available())
		})
	}
}

func (s *RingBufferSuite) TestWriteString() {
	tests := []struct {
		name    string
		size    uint64
		prefill string
		closed  bool
		data    string
		wantN   int
		wantErr error
		want    string
	}{
		{
			name:    "appends to unread bytes",
			size:    8,
			prefill: "head",
			data:    "tail",
			wantN:   4,
			want:    "headtail",
		},
		{
			name:    "overwrite drops oldest",
			size:    4,
			prefill: "--",
			data:    "keep",
			wantN:   4,
			want:    "keep",
		},
		{
			name:  "empty string",
			size:  4,
			data:  "",
			wantN: 0,
			want:  "",
		},
		{
			name:    "oversize rejected in overwrite mode",
			size:    4,
			data:    "too-long",
			wantN:   0,
			wantErr: ringbuffer.ErrTooLargeForWrite,
			want:    "",
		},
		{
			name:    "closed buffer rejects write",
			size:    8,
			closed:  true,
			data:    "late",
			wantN:   0,
			wantErr: io.EOF,
			want:    "",
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size)
			_, err := rb.WriteString(tt.prefill)
			s.Require().NoError(err)
			if tt.closed {
				s.Require().NoError(rb.Close())
			}
			n, err := rb.WriteString(tt.data)
			s.Require().ErrorIs(err, tt.wantErr)
			s.Require().Equal(tt.wantN, n)
			s.Require().Equal(tt.want, rb.String())
		})
	}
}

func (s *RingBufferSuite) TestWriteByte() {
	tests := []struct {
		name    string
		size    uint64
		closed  bool
		data    string
		wantErr error
		want    string
	}{
		{
			name: "bytes kept in order",
			size: 8,
			data: "ordered",
			want: "ordered",
		},
		{
			name: "overwrite drops oldest",
			size: 4,
			data: "overwrite",
			want: "rite",
		},
		{
			name:    "closed buffer rejects write",
			size:    8,
			closed:  true,
			data:    "late",
			wantErr: io.EOF,
			want:    "",
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size)
			if tt.closed {
				s.Require().NoError(rb.Close())
			}
			for i := range len(tt.data) {
				s.Require().ErrorIs(rb.WriteByte(tt.data[i]), tt.wantErr, "byte %d", i)
			}
			s.Require().Equal(tt.want, rb.String())
		})
	}
}

func (s *RingBufferSuite) TestWriteRune() {
	tests := []struct {
		name     string
		size     uint64
		blocking bool
		closed   bool
		r        rune
		wantN    int
		wantErr  error
		want     string
	}{
		{
			name:  "one-byte rune",
			size:  8,
			r:     'A',
			wantN: 1,
			want:  "A",
		},
		{
			name:  "two-byte rune",
			size:  8,
			r:     'é',
			wantN: 2,
			want:  "é",
		},
		{
			name:  "three-byte rune",
			size:  8,
			r:     '€',
			wantN: 3,
			want:  "€",
		},
		{
			name:  "four-byte rune",
			size:  8,
			r:     '🦉',
			wantN: 4,
			want:  "🦉",
		},
		{
			name:  "invalid rune writes replacement character",
			size:  8,
			r:     -1,
			wantN: 3,
			want:  "�",
		},
		{
			name:    "rune larger than capacity rejected in overwrite mode",
			size:    2,
			r:       '€',
			wantN:   0,
			wantErr: ringbuffer.ErrTooLargeForWrite,
			want:    "",
		},
		{
			name:     "rune larger than capacity rejected in blocking mode",
			size:     2,
			blocking: true,
			r:        '€',
			wantN:    0,
			wantErr:  ringbuffer.ErrTooLargeForWrite,
			want:     "",
		},
		{
			name:    "closed buffer rejects rune larger than capacity with EOF",
			size:    2,
			closed:  true,
			r:       '€',
			wantN:   0,
			wantErr: io.EOF,
			want:    "",
		},
		{
			name:     "closed blocking buffer rejects rune larger than capacity with EOF",
			size:     2,
			blocking: true,
			closed:   true,
			r:        '€',
			wantN:    0,
			wantErr:  io.EOF,
			want:     "",
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size, ringbuffer.WithOverWrite(!tt.blocking))
			if tt.closed {
				s.Require().NoError(rb.Close())
			}
			n, err := rb.WriteRune(tt.r)
			s.Require().ErrorIs(err, tt.wantErr)
			s.Require().Equal(tt.wantN, n)
			s.Require().Equal(tt.want, rb.String())
		})
	}
}

func (s *RingBufferSuite) TestReadRune() {
	tests := []struct {
		name     string
		size     uint64
		writes   []string
		skip     int
		closed   bool
		wantRune rune
		wantSize int
		wantErr  error
		wantLeft string
	}{
		{
			name:     "one-byte rune",
			size:     8,
			writes:   []string{"Ab"},
			wantRune: 'A',
			wantSize: 1,
			wantLeft: "b",
		},
		{
			name:     "four-byte rune",
			size:     8,
			writes:   []string{"🦉!"},
			wantRune: '🦉',
			wantSize: 4,
			wantLeft: "!",
		},
		{
			name:     "rune split across end of buffer",
			size:     4,
			writes:   []string{"ab", "€"},
			skip:     1,
			wantRune: '€',
			wantSize: 3,
			wantLeft: "",
		},
		{
			name:     "torn rune after overwrite yields replacement for one byte",
			size:     4,
			writes:   []string{"é", "€"},
			wantRune: utf8.RuneError,
			wantSize: 1,
			wantLeft: "€",
		},
		{
			name:     "invalid byte yields replacement for one byte",
			size:     8,
			writes:   []string{"\xffA"},
			wantRune: utf8.RuneError,
			wantSize: 1,
			wantLeft: "A",
		},
		{
			name:     "partial rune without wait reports empty and keeps bytes",
			size:     8,
			writes:   []string{"\xe2\x82"},
			wantErr:  ringbuffer.ErrEmpty,
			wantLeft: "\xe2\x82",
		},
		{
			name:     "partial rune after close yields replacement for one byte",
			size:     8,
			writes:   []string{"\xe2\x82"},
			closed:   true,
			wantRune: utf8.RuneError,
			wantSize: 1,
			wantLeft: "\x82",
		},
		{
			name:     "partial rune filling buffer yields replacement for one byte",
			size:     2,
			writes:   []string{"\xe2\x82"},
			wantRune: utf8.RuneError,
			wantSize: 1,
			wantLeft: "\x82",
		},
		{
			name:    "empty buffer",
			size:    8,
			wantErr: ringbuffer.ErrEmpty,
		},
		{
			name:    "empty buffer after close",
			size:    8,
			closed:  true,
			wantErr: io.EOF,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size)
			for _, w := range tt.writes {
				_, err := rb.WriteString(w)
				s.Require().NoError(err)
			}
			if tt.skip > 0 {
				_, err := rb.Read(make([]byte, tt.skip))
				s.Require().NoError(err)
			}
			if tt.closed {
				s.Require().NoError(rb.Close())
			}
			r, size, err := rb.ReadRune()
			s.Require().ErrorIs(err, tt.wantErr)
			s.Require().Equal(tt.wantRune, r)
			s.Require().Equal(tt.wantSize, size)
			s.Require().Equal(tt.wantLeft, rb.String())
		})
	}
}

func (s *RingBufferSuite) TestString() {
	tests := []struct {
		name    string
		size    uint64
		first   string
		readLen int
		second  string
		want    string
	}{
		{
			name: "empty buffer",
			size: 8,
			want: "",
		},
		{
			name:    "only unread bytes",
			size:    16,
			first:   "readunread",
			readLen: 4,
			want:    "unread",
		},
		{
			name:    "unread bytes across wrap",
			size:    4,
			first:   "---",
			readLen: 3,
			second:  "wrap",
			want:    "wrap",
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size)
			_, err := rb.WriteString(tt.first)
			s.Require().NoError(err)
			if tt.readLen > 0 {
				_, err = rb.Read(make([]byte, tt.readLen))
				s.Require().NoError(err)
			}
			_, err = rb.WriteString(tt.second)
			s.Require().NoError(err)

			s.Require().Equal(tt.want, rb.String())
			s.Require().Equal(len(tt.want), rb.Len(), "String does not consume")
		})
	}
}

func (s *RingBufferSuite) TestStringNilReceiver() {
	var rb *ringbuffer.RingBuffer
	s.Require().Equal("<nil>", rb.String())
}

func (s *RingBufferSuite) TestTruncate() {
	tests := []struct {
		name    string
		size    uint64
		first   string
		readLen int
		second  string
		keep    int
		after   string
		wantErr error
		want    string
	}{
		{
			name:  "keeps first n unread bytes",
			size:  8,
			first: "keepdrop",
			keep:  4,
			want:  "keep",
		},
		{
			name:  "n equal to Len keeps everything",
			size:  8,
			first: "all",
			keep:  3,
			want:  "all",
		},
		{
			name:  "zero discards everything",
			size:  8,
			first: "gone",
			keep:  0,
			want:  "",
		},
		{
			name:  "write after truncate appends to kept bytes",
			size:  8,
			first: "keepdrop",
			keep:  4,
			after: "next",
			want:  "keepnext",
		},
		{
			name:    "kept bytes across wrap",
			size:    4,
			first:   "--",
			readLen: 2,
			second:  "kedr",
			keep:    2,
			after:   "ep",
			want:    "keep",
		},
		{
			name:    "negative n rejected",
			size:    8,
			first:   "intact",
			keep:    -1,
			wantErr: ringbuffer.ErrTruncateOutOfRange,
			want:    "intact",
		},
		{
			name:    "n beyond Len rejected",
			size:    8,
			first:   "intact",
			keep:    7,
			wantErr: ringbuffer.ErrTruncateOutOfRange,
			want:    "intact",
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size)
			_, err := rb.WriteString(tt.first)
			s.Require().NoError(err)
			if tt.readLen > 0 {
				_, err = rb.Read(make([]byte, tt.readLen))
				s.Require().NoError(err)
			}
			_, err = rb.WriteString(tt.second)
			s.Require().NoError(err)

			s.Require().ErrorIs(rb.Truncate(tt.keep), tt.wantErr)
			_, err = rb.WriteString(tt.after)
			s.Require().NoError(err)
			s.Require().Equal(tt.want, rb.String())
		})
	}
}

func (s *RingBufferSuite) TestByteAndStringMethodsDoNotAllocate() {
	tests := []struct {
		name string
		op   func(rb *ringbuffer.RingBuffer) error
	}{
		{
			name: "WriteString",
			op: func(rb *ringbuffer.RingBuffer) error {
				_, err := rb.WriteString("no-alloc")
				return err
			},
		},
		{
			name: "WriteByte",
			op: func(rb *ringbuffer.RingBuffer) error {
				return rb.WriteByte('w')
			},
		},
		{
			name: "WriteRune",
			op: func(rb *ringbuffer.RingBuffer) error {
				_, err := rb.WriteRune('🦉')
				return err
			},
		},
		{
			name: "ReadByte",
			op: func(rb *ringbuffer.RingBuffer) error {
				if err := rb.WriteByte('r'); err != nil {
					return err
				}
				_, err := rb.ReadByte()
				return err
			},
		},
		{
			name: "ReadRune",
			op: func(rb *ringbuffer.RingBuffer) error {
				if _, err := rb.WriteRune('🦉'); err != nil {
					return err
				}
				_, _, err := rb.ReadRune()
				return err
			},
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(64)
			var err error
			allocs := testing.AllocsPerRun(100, func() {
				err = tt.op(rb)
			})
			s.Require().NoError(err)
			s.Require().Zero(allocs)
		})
	}
}

func (s *RingBufferSuite) TestIOReaderContract() {
	content := []byte("io.Reader contract")
	rb := ringbuffer.New(64)
	_, err := rb.Write(content)
	s.Require().NoError(err)
	s.Require().NoError(rb.Close())
	s.Require().NoError(iotest.TestReader(rb, content))
}

func (s *RingBufferSuite) TestStreamE2E() {
	src := make([]byte, 128<<10)
	_, _ = rand.NewChaCha8([32]byte{1}).Read(src)
	want := sha256.Sum256(src)

	same := func(r io.Reader) io.Reader { return r }
	tests := []struct {
		name     string
		size     uint64
		producer func(io.Reader) io.Reader
		consumer func(io.Reader) io.Reader
	}{
		{
			// bytes.Reader implements WriterTo, so this is one 128 KiB Write
			name:     "single write larger than buffer",
			size:     4096,
			producer: same,
			consumer: same,
		},
		{
			name:     "one-byte writes",
			size:     64,
			producer: iotest.OneByteReader,
			consumer: same,
		},
		{
			name:     "one-byte reads",
			size:     64,
			producer: same,
			consumer: iotest.OneByteReader,
		},
		{
			name:     "half-size reads and writes, non power of two",
			size:     1000,
			producer: iotest.HalfReader,
			consumer: iotest.HalfReader,
		},
		{
			name:     "capacity of one byte",
			size:     1,
			producer: same,
			consumer: same,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size,
				ringbuffer.WithOverWrite(false),
				ringbuffer.WithReadWaitOnEmpty(true),
			)
			produced := make(chan error, 1)
			go func() {
				_, err := io.Copy(rb, tt.producer(bytes.NewReader(src)))
				_ = rb.Close()
				produced <- err
			}()

			h := sha256.New()
			n, err := io.Copy(h, tt.consumer(rb))
			s.Require().NoError(err)
			s.Require().NoError(<-produced)
			s.Require().Equal(int64(len(src)), n)
			s.Require().Equal(want[:], h.Sum(nil))
		})
	}
}

const (
	fuzzWrite   byte = 0x00
	fuzzRead    byte = 0x80
	fuzzLenMask byte = 0x7f
)

func FuzzOverwriteModel(f *testing.F) {
	seeds := []struct {
		name         string
		capacityLog2 uint8
		payload      []byte
		ops          []byte
	}{
		{
			name:         "writes and reads that wrap and overwrite",
			capacityLog2: 2,
			payload:      []byte("wrap-and-overwrite"),
			ops: []byte{
				fuzzWrite | 3,
				fuzzRead | 2,
				fuzzWrite | 5,
				fuzzRead | 4,
				fuzzWrite | 4,
				fuzzWrite | 2,
				fuzzRead | 8,
			},
		},
		{
			name:         "one-byte capacity",
			capacityLog2: 0,
			payload:      []byte("one-byte-cap"),
			ops: []byte{
				fuzzWrite | 1,
				fuzzWrite | 1,
				fuzzRead | 1,
				fuzzWrite | 2,
				fuzzRead,
			},
		},
		{
			name:         "oversize writes rejected",
			capacityLog2: 6,
			payload:      bytes.Repeat([]byte("oversize-"), 20),
			ops: []byte{
				fuzzWrite | 100,
				fuzzRead | 16,
				fuzzWrite | 64,
				fuzzRead | 127,
			},
		},
	}
	for _, seed := range seeds {
		f.Add(seed.capacityLog2, seed.payload, seed.ops)
	}

	f.Fuzz(func(t *testing.T, capacityLog2 uint8, payload, ops []byte) {
		capacity := 1 << (capacityLog2 % 7) // power of two: New must not round it, or the model is wrong
		rb := ringbuffer.New(uint64(capacity))
		var model []byte

		for _, o := range ops {
			length := int(o & fuzzLenMask)
			isRead := o&fuzzRead != 0

			if !isRead {
				length = min(length, len(payload))
				chunk := payload[:length]
				payload = payload[length:]
				written, err := rb.Write(chunk)
				if length > capacity {
					require.ErrorIs(t, err, ringbuffer.ErrTooLargeForWrite)
					require.Zero(t, written)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, length, written)
				model = append(model, chunk...)
				if len(model) > capacity {
					model = model[len(model)-capacity:]
				}
				continue
			}

			dest := make([]byte, length)
			read, err := rb.Read(dest)
			switch {
			case length == 0:
				require.NoError(t, err)
				require.Zero(t, read)
			case len(model) == 0:
				require.ErrorIs(t, err, ringbuffer.ErrEmpty)
			default:
				expected := min(length, len(model))
				require.NoError(t, err)
				require.Equal(t, model[:expected], dest[:read])
				model = model[expected:]
			}
		}
	})
}
