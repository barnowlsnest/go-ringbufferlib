package ringbuffer

import (
	"errors"
)

func (rb *RingBuffer) Write(p []byte) (int, error) {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	if err := rb.validateState(); err != nil {
		return 0, err
	}
	switch {
	case rb.isEmpty():
		n, err := rb.write(p)
		if err != nil {
			return 0, err
		}
		rb.whenWrite.Broadcast()
		return n, nil
	case rb.isFull():
		if !rb.ShouldWriteWaitOnFull() {
			return 0, ErrFull
		}
		for rb.isFull() {
			rb.whenRead.Wait()
		}
		if err := rb.validateState(); err != nil {
			return 0, err
		}
		n, err := rb.write(p)
		if err != nil {
			return 0, err
		}
		return n, nil
	default:
		return rb.write(p)
	}
}

func (rb *RingBuffer) write(p []byte) (int, error) {
	if uint64(len(p)) > rb.size {
		return 0, ErrTooLargeForWrite
	}
	if len(p) == 0 {
		return 0, nil
	}
	if avail, err := rb.availWrite(p); err != nil {
		switch {
		case errors.Is(err, ErrNotEnoughToWrite):
			if !rb.ShouldOverWrite() {
				// if explicitly set not to overwrite we write as much we can and drop the rest
				// return number of written bytes and error
				return copy(rb.buf[rb.writePos:], p[:avail]), ErrFull
			}
			return rb.overwrite(avail, p), nil
		default:
			return 0, err
		}
	}
	n := copy(rb.buf[rb.writePos:], p)
	rb.writePos += uint64(n)
	rb.bytesCount += uint64(n)
	return n, nil
}

func (rb *RingBuffer) overwrite(avail uint64, p []byte) int {
	toOverWrite := uint64(len(p)) - avail
	n1 := copy(rb.buf[rb.writePos:], p[:avail])
	n2 := copy(rb.buf[:toOverWrite], p[avail:])
	rb.writePos = uint64(n2)
	return n1 + n2
}

func (rb *RingBuffer) isWriteOnLastByte() bool {
	return rb.writePos >= rb.size
}

func (rb *RingBuffer) availWrite(p []byte) (uint64, error) {
	pLen := uint64(len(p))
	if pLen > rb.size {
		return 0, ErrNotEnoughToWrite
	}
	n := rb.size - rb.writePos
	if n >= pLen {
		return n, nil
	}
	return n, ErrNotEnoughToWrite
}
