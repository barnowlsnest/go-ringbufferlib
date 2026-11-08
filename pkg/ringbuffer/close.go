package ringbuffer

func (rb *RingBuffer) Close() error {
	if rb.shouldDrainOnClose {
		rb.muState.Lock()
		rb.state = draining
		rb.muState.Unlock()
	}
	rb.muState.Lock()
	rb.state = closed
	rb.muState.Unlock()
	return nil
}
