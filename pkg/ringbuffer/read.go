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
	var n int
	switch {
	case rb.writePos > rb.readPos: // write ahead, read behind
		n = copy(dest, rb.buf[rb.readPos:rb.writePos])
		rb.readPos += uint64(n)
	case rb.writePos < rb.readPos: // read behind overwrite head
		left, over := rb.size-rb.readPos, rb.writePos
		n = copy(dest, rb.buf[rb.readPos:rb.size])
		rb.readPos += uint64(n)
		if uint64(len(dest)) > left {
			n += copy(dest[left:], rb.buf[:over])
			rb.readPos = uint64(n) - left
		}
	default: // read and write on the same pos
		n = 0
	}
	rb.bytesCount -= uint64(n)
	return n, nil
}
