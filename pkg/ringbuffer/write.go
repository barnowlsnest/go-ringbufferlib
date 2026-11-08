package ringbuffer

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
	for len(p) > 0 {
		for rb.isFull() && !rb.draining {
			rb.whenRead.Wait() // copyFrom broadcasts after each read
		}
		if err := rb.validateState(); err != nil {
			return total, err
		}
		free := rb.size - rb.bytesCount
		n := rb.copyIn(p[:min(uint64(len(p)), free)])
		total += n
		p = p[n:]
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
