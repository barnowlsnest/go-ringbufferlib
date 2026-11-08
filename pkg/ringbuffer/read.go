package ringbuffer

func (rb *RingBuffer) Read(dest []byte) (int, error) {
	rb.muState.Lock()
	defer rb.muState.Unlock()
	if err := rb.validateState(); err != nil {
		return 0, err
	}
	rb.wgReaders.Add(1)
	defer rb.wgReaders.Done()
	switch {
	case rb.isEmpty():
		if !rb.ShouldReadWaitOnEmpty() {
			return 0, ErrEmpty
		}
		for rb.isEmpty() {
			rb.whenWrite.Wait()
		}
		if err := rb.validateState(); err != nil {
			return 0, err
		}
		return rb.read(dest)
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
	return rb.copyFrom(dest), nil
}

func (rb *RingBuffer) copyFrom(dest []byte) int {
	n := min(uint64(len(dest)), rb.bytesCount)
	if n == 0 {
		return 0
	}
	c := copy(dest[:n], rb.buf[rb.readPos:])
	copy(dest[c:n], rb.buf)
	rb.readPos = (rb.readPos + n) & rb.mask
	rb.bytesCount -= n
	rb.whenRead.Broadcast()
	return int(n)
}
