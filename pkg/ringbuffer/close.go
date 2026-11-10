package ringbuffer

import (
	"time"
)

// Close is idempotent. With drain-on-close it waits up to drainTimeout for
// readers to empty the buffer.
func (rb *RingBuffer) Close() error {
	rb.muState.Lock()
	if rb.validateState() != nil {
		rb.muState.Unlock()
		return nil
	}
	if rb.ShouldDrainOnClose() {
		rb.setDraining()
		rb.whenRead.Broadcast()
		rb.whenWrite.Broadcast()
	}
	rb.muState.Unlock()
	if rb.ShouldDrainOnClose() {
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			rb.muState.Lock()
			for rb.bytesCount > 0 && !rb.closed {
				rb.whenRead.Wait()
			}
			rb.muState.Unlock()
		}()
		select {
		case <-drained:
		case <-time.After(rb.drainTimeout):
		}
	}
	rb.muState.Lock()
	if err := rb.validateContext(); err != nil {
		rb.muState.Unlock()
		return nil
	}
	if rb.stopCtx != nil {
		rb.stopCtx()
	}
	rb.setClosed()
	rb.setNotDraining()
	rb.whenRead.Broadcast()
	rb.whenWrite.Broadcast()
	rb.muState.Unlock()
	return nil
}
