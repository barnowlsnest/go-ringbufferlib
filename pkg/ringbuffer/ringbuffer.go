package ringbuffer

import (
	"errors"
	"io"
	"math/bits"
	"strings"
	"sync"
	"time"
)

var (
	ErrEmpty              = errors.New("empty: no new bytes to read")
	ErrDraining           = errors.New("drain: decline write")
	ErrTooLargeForWrite   = errors.New("write: write larger than buffer capacity")
	ErrTruncateOutOfRange = errors.New("truncate: n out of range")
)

const defaultBufferSize = 32 * 1024

type (
	Option func(rb *RingBuffer)

	RingBuffer struct {
		muState               *sync.Mutex
		whenRead              *sync.Cond
		whenWrite             *sync.Cond
		buf                   []byte
		size                  uint64
		mask                  uint64
		readPos               uint64
		writePos              uint64
		bytesCount            uint64
		drainTimeout          time.Duration
		draining              bool
		closed                bool
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
	for _, opt := range opts {
		opt(&rb)
	}
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

func (rb *RingBuffer) validateState() error {
	switch {
	case rb.isDraining():
		return ErrDraining
	case rb.isClosed():
		return io.EOF
	}
	return nil
}

func nextPow2(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len64(n-1)
}
