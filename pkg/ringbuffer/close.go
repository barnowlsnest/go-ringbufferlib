package ringbuffer

import (
	"time"
)

func (rb *RingBuffer) Close() error {
	if rb.ShouldDrainOnClose() {
		rb.muState.Lock()
		rb.setDraining()
		rb.muState.Unlock()
		readWaiterDone := make(chan struct{})
		go func() {
			defer close(readWaiterDone)
			rb.wgReaders.Wait()
		}()
		select {
		case <-readWaiterDone:
		case <-time.After(rb.drainTimeout):
		}
	}
	rb.muState.Lock()
	rb.setClosed()
	rb.muState.Unlock()
	return nil
}
