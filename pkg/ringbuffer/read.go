package ringbuffer

import (
	"io"
	"time"
	"unicode/utf8"
)

// Read keeps working while draining and after Close until the buffer is empty,
// then returns io.EOF.
func (rb *RingBuffer) Read(dest []byte) (int, error) {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	if len(dest) == 0 {
		return 0, nil
	}
	var deadline time.Time
	for rb.isEmpty() { // isEmpty is false once closed, so Close releases waiters
		if !rb.ShouldReadWaitOnEmpty() {
			return 0, ErrEmpty
		}
		if err := rb.waitOn(rb.whenWrite, rb.readTimeout, &deadline); err != nil {
			return 0, err
		}
	}
	if rb.abortErr != nil {
		return 0, rb.abortErr
	}
	if rb.closed && rb.bytesCount == 0 {
		return 0, io.EOF
	}
	return rb.read(dest)
}

func (rb *RingBuffer) ReadByte() (byte, error) {
	var b [1]byte
	if _, err := rb.Read(b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadRune decodes one UTF-8 rune. An invalid byte is consumed alone and
// returned as [utf8.RuneError] with size 1, like bytes.Buffer. When only part
// of a rune is buffered it returns [ErrEmpty] without consuming anything, or
// waits for the rest with WithReadWaitOnEmpty(true). After Close, while
// draining, or when the partial rune fills the buffer, the rest cannot arrive,
// so a partial rune is decoded as invalid. There is no UnreadRune.
func (rb *RingBuffer) ReadRune() (r rune, size int, err error) {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	var b [utf8.UTFMax]byte
	var deadline time.Time
	for {
		for rb.isEmpty() {
			if !rb.ShouldReadWaitOnEmpty() {
				return 0, 0, ErrEmpty
			}
			if err := rb.waitOn(rb.whenWrite, rb.readTimeout, &deadline); err != nil {
				return 0, 0, err
			}
		}
		if rb.abortErr != nil {
			return 0, 0, rb.abortErr
		}
		if rb.closed && rb.bytesCount == 0 {
			return 0, 0, io.EOF
		}
		n := rb.peek(b[:])
		if utf8.FullRune(b[:n]) || rb.closed || rb.draining || rb.bytesCount == rb.size {
			r, size = utf8.DecodeRune(b[:n])
			rb.copyFrom(b[:size])
			return r, size, nil
		}
		if !rb.ShouldReadWaitOnEmpty() {
			return 0, 0, ErrEmpty
		}
		if err := rb.waitOn(rb.whenWrite, rb.readTimeout, &deadline); err != nil {
			return 0, 0, err
		}
	}
}

func (rb *RingBuffer) peek(dest []byte) int {
	n := min(uint64(len(dest)), rb.bytesCount)
	first := min(n, rb.size-rb.readPos)
	c1 := copy(dest, rb.buf[rb.readPos:rb.readPos+first])
	return c1 + copy(dest[c1:], rb.buf[:n-first])
}

func (rb *RingBuffer) read(dest []byte) (int, error) {
	return rb.copyFrom(dest), nil
}

func (rb *RingBuffer) copyFrom(dest []byte) int {
	n := min(uint64(len(dest)), rb.bytesCount)
	if n == 0 {
		return 0
	}
	first := min(n, rb.size-rb.readPos)
	c1 := copy(dest, rb.buf[rb.readPos:rb.readPos+first])
	c2 := copy(dest[c1:], rb.buf[:n-first])
	rb.readPos = (rb.readPos + n) & rb.mask
	rb.bytesCount -= n
	rb.whenRead.Broadcast()
	return c1 + c2
}
