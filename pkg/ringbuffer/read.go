package ringbuffer

import (
	"errors"
	"io"
)

func (rb *RingBuffer) Read(dest []byte) (int, error) {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	if err := rb.validateState(); err != nil {
		return 0, err
	}
	switch {
	case rb.isEmpty():
		if !rb.ShouldReadWaitOnEmpty() {
			return 0, io.EOF
		}
		rb.wgReaders.Add(1)
		for !rb.shouldStopWaiting() {
			rb.whenWrite.Wait()
		}
		rb.wgReaders.Done()
		if err := rb.validateState(); err != nil {
			return 0, err
		}
		n, err := rb.read(dest)
		if err != nil {
			switch {
			case errors.Is(err, ErrEmpty):
				rb.setEmpty()
				return 0, nil
			default:
				return 0, err
			}
		}
		rb.setOK()
		return n, nil
	case rb.isFull():
		n, err := rb.read(dest)
		if err != nil {
			return 0, err
		}
		rb.whenRead.Broadcast()
		return n, nil
	default:
		return rb.read(dest)
	}
}

func (rb *RingBuffer) read(dest []byte) (int, error) {
	avail, pos, err := rb.availRead(dest)
	if err != nil {
		return 0, err
	}
	var n int
	switch pos {
	case writeAhead:
		n = copy(dest, rb.buf[rb.readPos:avail])
		rb.readPos += uint64(n)
	case readBehindOverwrite:
		left, over := rb.size-rb.readPos, rb.writePos
		n = copy(dest, rb.buf[rb.readPos:rb.size])
		rb.readPos += uint64(n)
		if uint64(len(dest)) > left {
			n += copy(dest[left:], rb.buf[:over])
			rb.readPos = uint64(n)
		}
	case readAndWriteEqual:
		n = 0
	}
	return n, nil
}

func (rb *RingBuffer) availRead(dest []byte) (n uint64, pos position, err error) {
	destLen := uint64(len(dest))
	if destLen > rb.size {
		return 0, errPos, ErrBiggerTheWholeBuffer
	}
	switch {
	case rb.isWriteAhead():
		return rb.writePos - rb.readPos, writeAhead, nil
	case rb.isReadBehindOverwrite():
		tail, head := rb.size-rb.readPos, rb.writePos
		return tail + head, readBehindOverwrite, nil
	default:
		return 0, readAndWriteEqual, ErrEmpty
	}
}

func (rb *RingBuffer) isWriteAhead() bool {
	return rb.writePos > rb.readPos
}

func (rb *RingBuffer) isReadBehindOverwrite() bool {
	return rb.writePos < rb.readPos
}
