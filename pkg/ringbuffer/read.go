package ringbuffer

import "io"

// Read keeps working while draining and after Close until the buffer is empty,
// then returns io.EOF.
func (rb *RingBuffer) Read(dest []byte) (int, error) {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	if len(dest) == 0 {
		return 0, nil
	}
	for rb.isEmpty() { // isEmpty is false once closed, so Close releases waiters
		if !rb.ShouldReadWaitOnEmpty() {
			return 0, ErrEmpty
		}
		rb.whenWrite.Wait()
	}
	if rb.closed && rb.bytesCount == 0 {
		return 0, io.EOF
	}
	return rb.read(dest)
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
