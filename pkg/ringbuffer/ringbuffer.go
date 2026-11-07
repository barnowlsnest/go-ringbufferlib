package ringbuffer

import (
	"errors"
	"fmt"
	"io"
	"sync"
	
	"golang.org/x/sync/errgroup"
)

var (
	ErrInvalidBuffer = errors.New("invalid ring buffer")
	ErrChunkTooLarge = errors.New("chunk is too large")
)

type (
	RingBuffer struct {
		chunkSize         uint64
		bufLen            uint64
		head              uint64
		tail              uint64
		bufBuckets        [][]byte
		onceClose         sync.Once
		onceDrained       sync.Once
		mu                sync.Mutex
		drainDone         chan struct{}
		draining          bool
		closed            bool
		shouldDrain       bool
		shouldBlockOnFull bool
	}
	
	Option func(rb *RingBuffer)
)

func WithChunkSize(chunkSize uint64) Option {
	return func(rb *RingBuffer) {
		rb.chunkSize = chunkSize
	}
}

func WithBufferLen(bufLen uint64) Option {
	return func(rb *RingBuffer) {
		rb.bufLen = bufLen
	}
}

func WithDrainingOnClose(shouldDrain bool) Option {
	return func(rb *RingBuffer) {
		rb.shouldDrain = shouldDrain
	}
}

func WithBlockingOnFull(shouldBlockOnFull bool) Option {
	return func(rb *RingBuffer) {
		rb.shouldBlockOnFull = shouldBlockOnFull
	}
}

func New(opts ...Option) (*RingBuffer, error) {
	var rb RingBuffer
	for _, opt := range opts {
		opt(&rb)
	}
	if err := rb.validate(); err != nil {
		return nil, err
	}
	rb.bufBuckets = make([][]byte, rb.bufLen)
	if rb.shouldDrain {
		rb.drainDone = make(chan struct{}, rb.bufLen)
	}
	return &rb, nil
}

func (rb *RingBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(len(p)) > rb.chunkSize {
		return 0, ErrChunkTooLarge
	}
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.draining && rb.shouldDrain {
		return 0, io.ErrClosedPipe
	}
	if rb.closed {
		return 0, io.ErrClosedPipe
	}
	if rb.head == rb.bufLen {
		rb.head = 0
	}
	if rb.bufBuckets[rb.head] == nil {
		rb.bufBuckets[rb.head] = make([]byte, rb.chunkSize)
	}
	n := copy(rb.bufBuckets[rb.head], p)
	rb.head++
	return n, nil
}

func (rb *RingBuffer) Read(b []byte) (int, error) {
	rb.mu.Lock()
	if rb.shouldDrain {
		drained := rb.draining && rb.tail == rb.head
		if drained {
			rb.mu.Unlock()
			rb.onceDrained.Do(func() { close(rb.drainDone) })
			return 0, io.EOF
		}
	} else if rb.closed {
		rb.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if rb.tail == rb.bufLen {
		rb.tail = 0
	}
	if rb.head == 0 {
		rb.mu.Unlock()
		return 0, io.EOF
	}
	n := copy(b, rb.bufBuckets[rb.tail])
	rb.tail++
	rb.mu.Unlock()
	if rb.shouldDrain {
		rb.drainDone <- struct{}{}
	}
	return n, nil
}

func (rb *RingBuffer) Close() error {
	rb.mu.Lock()
	if rb.bufBuckets == nil {
		rb.mu.Unlock()
		return nil
	}
	rb.closed = true
	rb.mu.Unlock()
	if !rb.shouldDrain {
		rb.mu.Lock()
		rb.bufBuckets = nil
		rb.mu.Unlock()
		return nil
	}
	var g errgroup.Group
	g.SetLimit(1)
	rb.onceClose.Do(func() {
		g.Go(func() error {
			rb.mu.Lock()
			rb.draining = true
			rb.mu.Unlock()
			for range rb.drainDone {
			}
			rb.mu.Lock()
			rb.bufBuckets = nil
			rb.mu.Unlock()
			return nil
		})
	})
	return g.Wait()
}

func (rb *RingBuffer) validate() error {
	if rb.chunkSize <= 0 {
		return fmt.Errorf("%w: buffer chunk size must be set", ErrInvalidBuffer)
	}
	if rb.bufLen <= 0 {
		return fmt.Errorf("%w: buffer len must be set", ErrInvalidBuffer)
	}
	return nil
}
