package ringbuffer

import (
	"errors"
	"fmt"
	"io"
	"sync"
	
	"golang.org/x/sync/errgroup"
)

var ErrInvalidBuffer = errors.New("invalid ring buffer")

type (
	RingBuffer struct {
		chunkSize   uint64
		bufLen      uint64
		head        uint64
		tail        uint64
		bufBuckets  [][]byte
		onceClose   sync.Once
		mu          sync.Mutex
		drainDone   chan struct{}
		draining    bool
		shouldDrain bool
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
	if rb.draining && rb.shouldDrain {
		return 0, io.ErrClosedPipe
	}
	if !rb.shouldDrain {
		return 0, io.ErrClosedPipe
	}
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.head == rb.bufLen {
		rb.head = 0
	}
	if rb.bufBuckets[rb.head] == nil {
		rb.bufBuckets[rb.head] = make([]byte, rb.chunkSize)
	}
	copy(rb.bufBuckets[rb.head], p)
	rb.head++
	return 0, nil
}

func (rb *RingBuffer) Read(b []byte) (int, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.shouldDrain {
		drained := rb.draining && rb.tail == rb.head
		if drained {
			close(rb.drainDone)
			return 0, io.EOF
		}
	} else {
		return 0, io.ErrClosedPipe
	}
	
	if rb.tail == rb.bufLen {
		rb.tail = 0
	}
	if rb.head == 0 {
		return 0, io.EOF
	}
	copy(b, rb.bufBuckets[rb.tail])
	rb.tail++
	if rb.shouldDrain {
		rb.drainDone <- struct{}{}
	}
	return 0, nil
}

func (rb *RingBuffer) Close() error {
	rb.mu.Lock()
	if rb.bufBuckets == nil {
		rb.mu.Unlock()
		return nil
	}
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
