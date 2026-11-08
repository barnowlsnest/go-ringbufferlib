package ringbuffer

func (rb *RingBuffer) Close() error {
	if rb.ShouldDrainOnClose() {
		rb.muState.Lock()
		rb.setDraining()
		rb.muState.Unlock()
	}
	rb.muState.Lock()
	rb.setClosed()
	rb.muState.Unlock()
	return nil
}
