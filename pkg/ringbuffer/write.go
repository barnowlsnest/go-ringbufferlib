package ringbuffer

import (
	"time"
	"unicode/utf8"
)

func (rb *RingBuffer) Write(p []byte) (int, error) {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	if err := rb.validateState(); err != nil {
		return 0, err
	}
	if rb.ShouldOverWrite() {
		return rb.write(p)
	}
	var total int
	var deadline time.Time
	for len(p) > 0 {
		for rb.isFull() && !rb.draining {
			// copyFrom broadcasts after each read
			if err := rb.waitOn(&rb.writeWake, rb.writeTimeout, &deadline); err != nil {
				return total, err
			}
			if err := rb.validateState(); err != nil {
				return total, err
			}
		}
		free := rb.size - rb.bytesCount
		n := rb.copyIn(p[:min(uint64(len(p)), free)])
		total += n
		p = p[n:]
		deadline = time.Time{}
	}
	return total, nil
}

func (rb *RingBuffer) write(p []byte) (int, error) {
	pLen := uint64(len(p))
	if pLen == 0 {
		return 0, nil
	}
	if pLen > rb.size {
		return 0, ErrTooLargeForWrite
	}
	if free := rb.size - rb.bytesCount; pLen > free {
		drop := pLen - free
		rb.readPos = (rb.readPos + drop) & rb.mask
		rb.bytesCount -= drop
	}
	return rb.copyIn(p), nil
}

func (rb *RingBuffer) copyIn(p []byte) int {
	n := copy(rb.buf[rb.writePos:], p)
	copy(rb.buf, p[n:])
	rb.writePos = (rb.writePos + uint64(len(p))) & rb.mask
	rb.bytesCount += uint64(len(p))
	rb.whenWrite.Broadcast()
	return len(p)
}

func (rb *RingBuffer) WriteString(s string) (int, error) {
	p := []byte(s)
	return rb.Write(p)
}

func (rb *RingBuffer) WriteByte(c byte) error {
	b := [1]byte{c}
	_, err := rb.Write(b[:])
	return err
}

// WriteRune writes the UTF-8 encoding of r; an invalid rune is written as
// [utf8.RuneError], like bytes.Buffer. A rune whose encoding is longer than
// Cap() fails with [ErrTooLargeForWrite] in every mode.
func (rb *RingBuffer) WriteRune(r rune) (int, error) {
	var b [utf8.UTFMax]byte
	n := utf8.EncodeRune(b[:], r)
	rb.muState.Lock()
	err := rb.validateState()
	rb.muState.Unlock()
	if err != nil {
		return 0, err
	}
	if n > rb.Cap() {
		return 0, ErrTooLargeForWrite
	}
	return rb.Write(b[:n])
}
