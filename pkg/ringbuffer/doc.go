// Package ringbuffer provides a fixed-capacity byte ring buffer that is safe
// for concurrent use and implements [io.Reader], [io.Writer], [io.Closer],
// [io.ByteReader], [io.ByteWriter], [io.RuneReader], [io.StringWriter] and
// [fmt.Stringer].
//
// Like bytes.Buffer it also has Len, Cap, Available, Reset, Truncate and
// WriteRune. Truncate returns [ErrTruncateOutOfRange] where bytes.Buffer
// panics. There is no UnreadByte or UnreadRune.
//
// The buffer stores bytes, not runes. Overwriting and Truncate can cut a
// multi-byte rune, and ReadRune then returns [utf8.RuneError] for each
// leftover byte.
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
// # Timeouts
//
// WithReadTimeout and WithWriteTimeout bound how long a blocked call waits,
// failing it with [ErrTimeout]. They limit waiting without progress, not the
// whole stream: each Read starts a fresh wait, and a Write restarts its wait
// whenever part of p is written, so io.Copy over a slow but steady stream
// keeps going. Calls that do not block are unaffected.
//
// # Cancellation
//
// WithContext ties the buffer to a context. Once it is done, blocked calls
// wake at once and every call fails with context.Cause(ctx), even with unread
// bytes left. That tells a canceled stream apart from one that ended with
// Close and [io.EOF]. Timeouts catch a stalled stream; the context stops a
// healthy one.
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
