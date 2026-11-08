package ringbuffer

import (
	"io"
)

type BufferState int

const (
	empty BufferState = iota
	ok
	full
	draining
	closed
)

func (rb *RingBuffer) isFull() bool {
	return rb.state == full
}

func (rb *RingBuffer) isOK() bool {
	return rb.state == ok
}

func (rb *RingBuffer) setOK() {
	rb.state = ok
}

func (rb *RingBuffer) setFull() {
	rb.state = full
}

func (rb *RingBuffer) isEmpty() bool {
	return rb.state == draining
}

func (rb *RingBuffer) setEmpty() {
	rb.state = empty
}

func (rb *RingBuffer) isDraining() bool {
	return rb.state == draining
}

func (rb *RingBuffer) setDraining() {
	rb.state = draining
}

func (rb *RingBuffer) isClosed() bool {
	return rb.state == closed
}

func (rb *RingBuffer) setClosed() {
	rb.state = closed
}

func (rb *RingBuffer) validateState() error {
	switch {
	case rb.isDraining(), rb.isClosed():
		return io.ErrClosedPipe
	default:
		return nil
	}
}

func (rb *RingBuffer) shouldStopWaiting() bool {
	return rb.isOK() || rb.isClosed()
}
