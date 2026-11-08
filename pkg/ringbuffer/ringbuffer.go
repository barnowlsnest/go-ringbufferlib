package ringbuffer

import (
	"errors"
	"sync"
)

var (
	ErrBiggerTheWholeBuffer = errors.New("buffer is too small")
	ErrNotEnoughToWrite     = errors.New("write: not enough bytes")
	ErrEmpty                = errors.New("empty: no new bytes to read")
	ErrFull                 = errors.New("full: no free bytes to write to")
)

const (
	writeAhead position = iota
	readBehindOverwrite
	readAndWriteEqual
)

const errPos position = -1

type (
	Option func(rb *RingBuffer)
	
	RingBuffer struct {
		size                  uint64
		readPos               uint64
		writePos              uint64
		state                 BufferState
		muState               *sync.Mutex
		whenRead              *sync.Cond
		whenWrite             *sync.Cond
		buf                   []byte
		shouldDrainOnClose    bool
		shouldReadWaitOnEmpty bool
		shouldWriteWaitOnFull bool
		shouldOverWrite       bool
	}
	
	position int
)

func New(size uint64, opts ...Option) *RingBuffer {
	var muState sync.Mutex
	rb := RingBuffer{
		size:                  size,
		buf:                   make([]byte, size),
		muState:               &muState,
		whenRead:              sync.NewCond(&muState),
		whenWrite:             sync.NewCond(&muState),
		state:                 empty,
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

func WithDrainOnClose(shouldDrainOnClose bool) Option {
	return func(rb *RingBuffer) {
		rb.shouldDrainOnClose = shouldDrainOnClose
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
