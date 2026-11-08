package ringbuffer

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
	pLen := uint64(len(p))
	if pLen == 0 {
		return 0, nil
	}
	if pLen > rb.size {
		return 0, ErrTooLargeForWrite
	}
	if len(p) == 0 {
		return 0, nil
	}
	free := rb.size - rb.bytesCount
	if pLen <= free {
		return rb.copyIn(p), nil
	}
	if rb.ShouldOverWrite() {
		drop := pLen - free
		rb.readPos = (rb.readPos + drop) & rb.mask
		rb.bytesCount -= drop
		return rb.copyIn(p), nil
	}
	return rb.copyIn(p[:free]), ErrFull
}

func (rb *RingBuffer) overwrite(avail uint64, p []byte) int {
	toOverWrite := uint64(len(p)) - avail
	n1 := copy(rb.buf[rb.writePos:], p[:avail])
	n2 := copy(rb.buf[:toOverWrite], p[avail:])
	rb.writePos = uint64(n2)
	return n1 + n2
}

func (rb *RingBuffer) copyIn(p []byte) int {
	n := copy(rb.buf[rb.writePos:], p)
	copy(rb.buf, p[n:])
	rb.writePos = (rb.writePos + uint64(len(p))) & rb.mask
	rb.bytesCount += uint64(len(p))
	rb.whenWrite.Broadcast()
	return len(p)
}

func (rb *RingBuffer) isWriteOnLastByte() bool {
	return rb.writePos >= rb.size
}
