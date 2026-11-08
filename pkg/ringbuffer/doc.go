// Package ringbuffer provides a fixed-capacity byte ring buffer that is safe
// for concurrent use and implements [io.Reader], [io.Writer] and [io.Closer].
//
// Capacity is rounded up to the next power of two; New(0) uses 32 KiB.
//
// # Writes
//
// By default Write never blocks: when the buffer is full it drops the oldest
// unread bytes. A single write larger than the capacity fails with
// [ErrTooLargeForWrite].
//
// With WithOverWrite(false), nothing is dropped: Write blocks until all of p is
// written, so p may be larger than the capacity and streams through as readers
// free space.
//
// # Reads
//
// Read returns up to len(dest) bytes in FIFO order. On an empty buffer it
// returns [ErrEmpty], or blocks until data arrives with WithReadWaitOnEmpty(true).
//
// # Close
//
// Close wakes every blocked reader and writer. Readers still receive the
// remaining bytes, then [io.EOF]; writes fail with [io.EOF].
//
// With WithDrainOnClose(true, timeout), Close first rejects writes with
// [ErrDraining] and waits up to timeout for readers to empty the buffer.
// Close is idempotent.
//
// # Streaming
//
//	func stream(dst io.Writer, src io.Reader) error {
//		rb := ringbuffer.New(64<<10,
//			ringbuffer.WithOverWrite(false),
//			ringbuffer.WithReadWaitOnEmpty(true),
//		)
//
//		produced := make(chan error, 1)
//		go func() {
//			_, err := io.Copy(rb, src)
//			produced <- errors.Join(err, rb.Close())
//		}()
//
//		if _, err := io.Copy(dst, rb); err != nil {
//			// Close unblocks the producer so it can exit.
//			closeErr := rb.Close()
//			<-produced
//			return errors.Join(fmt.Errorf("consume: %w", err), closeErr)
//		}
//		if err := <-produced; err != nil {
//			return fmt.Errorf("produce: %w", err)
//		}
//		return nil
//	}
package ringbuffer
