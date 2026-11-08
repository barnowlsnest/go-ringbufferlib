package ringbuffer

import (
	"errors"
	"io"
	"math/bits"
	"sync"
	"time"
)

var (
	ErrEmpty            = errors.New("empty: no new bytes to read")
	ErrFull             = errors.New("full: no free bytes to write to")
	ErrDraining         = errors.New("drain: decline write")
	ErrTooLargeForWrite = errors.New("write: write larger than buffer capacity")
)

const defaultBufferSize = 32 * 1024

type (
	Option func(rb *RingBuffer)
	
	RingBuffer struct {
		size                  uint64
		mask                  uint64
		readPos               uint64
		writePos              uint64
		bytesCount            uint64
		drainTimeout          time.Duration
		wgReaders             sync.WaitGroup
		muState               *sync.Mutex
		whenRead              *sync.Cond
		whenWrite             *sync.Cond
		buf                   []byte
		draining              bool
		closed                bool
		shouldDrainOnClose    bool
		shouldReadWaitOnEmpty bool
		shouldWriteWaitOnFull bool
		shouldOverWrite       bool
	}
	
	position int
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
		shouldWriteWaitOnFull: false,
	}
	for _, opt := range opts {
		opt(&rb)
	}
	return &rb
}

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

func WithWriteWaitOnFull(shouldWriteWaitOnFull bool) Option {
	return func(rb *RingBuffer) {
		rb.shouldWriteWaitOnFull = shouldWriteWaitOnFull
	}
}

func (rb *RingBuffer) ShouldDrainOnClose() bool {
	return rb.shouldDrainOnClose
}

func (rb *RingBuffer) ShouldReadWaitOnEmpty() bool {
	return rb.shouldReadWaitOnEmpty
}

func (rb *RingBuffer) ShouldWriteWaitOnFull() bool {
	return rb.shouldWriteWaitOnFull
}

func (rb *RingBuffer) ShouldOverWrite() bool {
	return rb.shouldOverWrite
}

func (rb *RingBuffer) isEmpty() bool {
	return rb.bytesCount == 0 && !rb.closed
}

func (rb *RingBuffer) isFull() bool {
	return rb.bytesCount == rb.size && !rb.closed
}

func (rb *RingBuffer) setDraining() {
	rb.draining = true
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
