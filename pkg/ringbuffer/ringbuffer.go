package ringbuffer

import (
	"context"
	"errors"
	"io"
	"math/bits"
	"runtime"
	"strings"
	"sync"
	"time"
	"weak"
)

var (
	ErrEmpty              = errors.New("empty: no new bytes to read")
	ErrDraining           = errors.New("drain: decline write")
	ErrTooLargeForWrite   = errors.New("write: write larger than buffer capacity")
	ErrTruncateOutOfRange = errors.New("truncate: n out of range")
	ErrTimeout            = errors.New("timeout: no progress within the timeout")
)

const defaultBufferSize = 32 * 1024

type (
	Option func(rb *RingBuffer)

	// condTimer wakes every waiter on cond at the earliest deadline among them,
	// so timed waits reuse one timer per cond instead of one per wait.
	condTimer struct {
		cond    *sync.Cond
		timer   *time.Timer
		at      time.Time
		waiters int
	}

	RingBuffer struct {
		muState               *sync.Mutex
		whenRead              *sync.Cond
		whenWrite             *sync.Cond
		abortErr              error
		ctx                   context.Context
		ctxDone               <-chan struct{}
		stopCtx               func() bool
		readWake              condTimer
		writeWake             condTimer
		buf                   []byte
		ctxCleanup            runtime.Cleanup
		ctxGeneration         uint64
		size                  uint64
		mask                  uint64
		readPos               uint64
		writePos              uint64
		bytesCount            uint64
		drainTimeout          time.Duration
		readTimeout           time.Duration
		writeTimeout          time.Duration
		draining              bool
		closed                bool
		built                 bool
		shouldDrainOnClose    bool
		shouldReadWaitOnEmpty bool
		shouldOverWrite       bool
	}
)

func New(size uint64, opts ...Option) *RingBuffer {
	if size == 0 {
		size = defaultBufferSize
	}
	var muState sync.Mutex
	size = nextPow2(size)
	rb := RingBuffer{
		size:                  size,
		mask:                  size - 1,
		buf:                   make([]byte, size),
		muState:               &muState,
		whenRead:              sync.NewCond(&muState),
		whenWrite:             sync.NewCond(&muState),
		shouldOverWrite:       true,
		shouldDrainOnClose:    false,
		shouldReadWaitOnEmpty: false,
	}
	rb.readWake.cond = rb.whenWrite
	rb.writeWake.cond = rb.whenRead
	for _, opt := range opts {
		opt(&rb)
	}
	rb.built = true
	return &rb
}

// WithOverWrite with true drops whole bytes when full, so text may then begin
// with part of a rune.
func WithOverWrite(shouldOverWrite bool) Option {
	return func(rb *RingBuffer) {
		rb.shouldOverWrite = shouldOverWrite
	}
}

func WithDrainOnClose(shouldDrainOnClose bool, drainTimeout time.Duration) Option {
	return func(rb *RingBuffer) {
		rb.shouldDrainOnClose = shouldDrainOnClose
		rb.drainTimeout = drainTimeout
	}
}

func WithReadWaitOnEmpty(shouldReadWaitOnEmpty bool) Option {
	return func(rb *RingBuffer) {
		rb.shouldReadWaitOnEmpty = shouldReadWaitOnEmpty
	}
}

// WithReadTimeout limits how long one blocked Read, ReadByte or ReadRune waits
// for data before failing with [ErrTimeout]. A timeout of zero or less waits
// forever.
func WithReadTimeout(timeout time.Duration) Option {
	return func(rb *RingBuffer) {
		rb.readTimeout = timeout
	}
}

// WithWriteTimeout limits how long a blocked Write waits for room before
// failing with [ErrTimeout]. The wait restarts each time some of p is written,
// so a long stream fails only after timeout without progress. A timeout of
// zero or less waits forever.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(rb *RingBuffer) {
		rb.writeTimeout = timeout
	}
}

// WithContext aborts the buffer once ctx is done: blocked calls wake at once
// and every later call fails with context.Cause(ctx), even with unread bytes
// left. Canceling after Close has no effect. A nil ctx is ignored, and a later
// WithContext replaces an earlier one.
func WithContext(ctx context.Context) Option {
	return func(rb *RingBuffer) {
		if ctx == nil {
			return
		}
		rb.muState.Lock()
		defer rb.muState.Unlock()
		if rb.closed {
			return
		}
		if rb.stopCtx != nil {
			rb.stopCtx()
			rb.stopCtx = nil
			rb.ctxCleanup.Stop()
			rb.ctxCleanup = runtime.Cleanup{}
		}
		rb.ctxGeneration++
		generation := rb.ctxGeneration
		rb.ctx = ctx
		rb.ctxDone = ctx.Done()
		// Held weakly so a long-lived ctx does not keep an abandoned buffer alive.
		abandonable := weak.Make(rb)
		abort := func() {
			live := abandonable.Value()
			if live == nil {
				return
			}
			live.muState.Lock()
			defer live.muState.Unlock()
			if live.ctxGeneration != generation {
				return
			}
			live.abort(context.Cause(ctx))
		}
		// Inside New a later WithContext may still replace a done ctx, and
		// AfterFunc would abort from its own goroutine before that. Nothing
		// can be blocked yet, so validateContext aborts on the first call.
		if ctx.Err() != nil {
			if rb.built {
				rb.abort(context.Cause(ctx))
			}
			return
		}
		stop := context.AfterFunc(ctx, abort)
		rb.stopCtx = stop
		rb.ctxCleanup = runtime.AddCleanup(rb, func(stop func() bool) { stop() }, stop)
	}
}

func (rb *RingBuffer) ShouldDrainOnClose() bool {
	return rb.shouldDrainOnClose
}

func (rb *RingBuffer) ShouldReadWaitOnEmpty() bool {
	return rb.shouldReadWaitOnEmpty
}

func (rb *RingBuffer) ShouldOverWrite() bool {
	return rb.shouldOverWrite
}

// Len returns the number of unread bytes.
func (rb *RingBuffer) Len() int {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	return rb.unreadLen()
}

// Cap returns the capacity, after rounding up to a power of two.
func (rb *RingBuffer) Cap() int {
	return len(rb.buf)
}

// Available returns how many bytes can be written without dropping or
// blocking.
func (rb *RingBuffer) Available() int {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	return rb.Cap() - rb.unreadLen()
}

// Reset discards all unread bytes. It does not reopen a closed buffer.
func (rb *RingBuffer) Reset() {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	rb.readPos, rb.writePos, rb.bytesCount = 0, 0, 0
	rb.whenRead.Broadcast()
}

// Truncate discards all but the first n unread bytes. Unlike bytes.Buffer it
// returns [ErrTruncateOutOfRange] instead of panicking when n < 0 or n > Len.
func (rb *RingBuffer) Truncate(n int) error {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	if n < 0 || uint64(n) > rb.bytesCount {
		return ErrTruncateOutOfRange
	}
	rb.bytesCount = uint64(n)
	rb.writePos = (rb.readPos + rb.bytesCount) & rb.mask
	rb.whenRead.Broadcast()
	return nil
}

// String returns a copy of the unread bytes without consuming them.
func (rb *RingBuffer) String() string {
	if rb == nil {
		return "<nil>"
	}
	rb.muState.Lock()
	defer rb.muState.Unlock()
	first := min(rb.bytesCount, rb.size-rb.readPos)
	var sb strings.Builder
	sb.Grow(rb.unreadLen())
	sb.Write(rb.buf[rb.readPos : rb.readPos+first])
	sb.Write(rb.buf[:rb.bytesCount-first])
	return sb.String()
}

func (rb *RingBuffer) isEmpty() bool {
	return rb.bytesCount == 0 && !rb.closed
}

func (rb *RingBuffer) isFull() bool {
	return rb.bytesCount == rb.size && !rb.closed
}

// bytesCount never exceeds len(buf), so this is bytesCount as an int with no
// overflowing conversion.
func (rb *RingBuffer) unreadLen() int {
	return len(rb.buf[:rb.bytesCount])
}

func (rb *RingBuffer) setDraining() {
	rb.draining = true
}

func (rb *RingBuffer) setNotDraining() {
	rb.draining = false
}

func (rb *RingBuffer) setClosed() {
	rb.closed = true
}

func (rb *RingBuffer) isDraining() bool {
	return rb.draining
}

func (rb *RingBuffer) isClosed() bool {
	return rb.closed
}

func (rb *RingBuffer) abort(cause error) {
	if rb.closed {
		return
	}
	rb.abortErr = cause
	rb.setClosed()
	rb.setNotDraining()
	rb.whenRead.Broadcast()
	rb.whenWrite.Broadcast()
}

func (rb *RingBuffer) validateState() error {
	if err := rb.validateContext(); err != nil {
		return err
	}
	switch {
	case rb.isDraining():
		return ErrDraining
	case rb.isClosed():
		return io.EOF
	}
	return nil
}

func (rb *RingBuffer) validateContext() error {
	if rb.abortErr != nil {
		return rb.abortErr
	}
	if rb.ctxDone == nil || rb.closed {
		return nil
	}
	// A receive on the cached Done channel costs the same at any ctx depth,
	// while ctx.Err() walks the whole chain.
	select {
	case <-rb.ctxDone:
		rb.abort(context.Cause(rb.ctx))
		return rb.abortErr
	default:
		return nil
	}
}

// waitOn sets *deadline on its first call, so one deadline covers every wait
// of a call until the caller resets it to zero. It re-checks the context after
// waking, so callers need not poll it again.
func (rb *RingBuffer) waitOn(t *condTimer, timeout time.Duration, deadline *time.Time) error {
	if timeout <= 0 {
		t.cond.Wait()
		return rb.validateContext()
	}
	if deadline.IsZero() {
		*deadline = time.Now().Add(timeout)
	}
	left := time.Until(*deadline)
	if left <= 0 {
		return ErrTimeout
	}
	t.arm(*deadline, left)
	t.waiters++
	t.cond.Wait()
	t.waiters--
	if t.waiters == 0 {
		t.disarm()
	}
	return rb.validateContext()
}

// arm moves the timer earlier when deadline comes before the one it holds.
// Waiters with a later deadline wake early, see time left, and re-arm.
func (t *condTimer) arm(deadline time.Time, left time.Duration) {
	if !t.at.IsZero() && !t.at.After(deadline) {
		return
	}
	t.at = deadline
	if t.timer == nil {
		t.timer = time.AfterFunc(left, t.fire)
		return
	}
	t.timer.Reset(left)
}

// disarm stops the timer once nobody waits, so it neither fires for nobody
// nor keeps the buffer reachable.
func (t *condTimer) disarm() {
	t.at = time.Time{}
	if t.timer != nil {
		t.timer.Stop()
	}
}

func (t *condTimer) fire() {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	t.at = time.Time{}
	t.cond.Broadcast()
}

func nextPow2(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len64(n-1)
}
