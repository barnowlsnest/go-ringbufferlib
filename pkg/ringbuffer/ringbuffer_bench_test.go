package ringbuffer_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/barnowlsnest/go-ringbufferlib/v1/pkg/ringbuffer"
)

func BenchmarkWriteRead(b *testing.B) {
	benchmarks := []struct {
		name  string
		chunk int
	}{
		{
			name:  "16B",
			chunk: 16,
		},
		{
			name:  "256B",
			chunk: 256,
		},
		{
			name:  "4KiB",
			chunk: 4 << 10,
		},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			rb := ringbuffer.New(64 << 10)
			chunk := make([]byte, bm.chunk)
			dest := make([]byte, bm.chunk)
			b.SetBytes(int64(bm.chunk))
			b.ReportAllocs()
			for b.Loop() {
				_, _ = rb.Write(chunk)
				_, _ = rb.Read(dest)
			}
		})
	}
}

func BenchmarkStream(b *testing.B) {
	const total = 1 << 20

	newRingBuffer := func(size uint64) func() (io.Reader, io.Writer) {
		return func() (io.Reader, io.Writer) {
			rb := ringbuffer.New(size,
				ringbuffer.WithOverWrite(false),
				ringbuffer.WithReadWaitOnEmpty(true),
			)
			return rb, rb
		}
	}
	newTimedRingBuffer := func(size uint64) func() (io.Reader, io.Writer) {
		return func() (io.Reader, io.Writer) {
			rb := ringbuffer.New(size,
				ringbuffer.WithOverWrite(false),
				ringbuffer.WithReadWaitOnEmpty(true),
				ringbuffer.WithReadTimeout(time.Minute),
				ringbuffer.WithWriteTimeout(time.Minute),
			)
			return rb, rb
		}
	}
	newPipe := func() (io.Reader, io.Writer) {
		return io.Pipe()
	}

	benchmarks := []struct {
		name  string
		pipe  func() (io.Reader, io.Writer)
		chunk int
	}{
		{
			name:  "ringbuffer/4KiB/chunk=64B",
			pipe:  newRingBuffer(4 << 10),
			chunk: 64,
		},
		{
			name:  "ringbuffer/4KiB/chunk=512B",
			pipe:  newRingBuffer(4 << 10),
			chunk: 512,
		},
		{
			name:  "ringbuffer/4KiB/chunk=4KiB",
			pipe:  newRingBuffer(4 << 10),
			chunk: 4 << 10,
		},
		{
			name:  "ringbuffer/64KiB/chunk=4KiB",
			pipe:  newRingBuffer(64 << 10),
			chunk: 4 << 10,
		},
		{
			name:  "ringbuffer/4KiB/chunk=64B/timeouts",
			pipe:  newTimedRingBuffer(4 << 10),
			chunk: 64,
		},
		{
			name:  "ringbuffer/4KiB/chunk=64KiB/timeouts",
			pipe:  newTimedRingBuffer(4 << 10),
			chunk: 64 << 10,
		},
		{
			name:  "io.Pipe/chunk=512B",
			pipe:  newPipe,
			chunk: 512,
		},
		{
			name:  "io.Pipe/chunk=4KiB",
			pipe:  newPipe,
			chunk: 4 << 10,
		},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			r, w := bm.pipe()
			chunk := make([]byte, bm.chunk)
			dest := make([]byte, bm.chunk)
			b.SetBytes(total)
			b.ReportAllocs()
			for b.Loop() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					for sent := 0; sent < total; sent += len(chunk) {
						_, _ = w.Write(chunk)
					}
				}()
				for received := 0; received < total; {
					n, _ := r.Read(dest)
					received += n
				}
				<-done
			}
		})
	}
}

type requestKey int

func BenchmarkWriteReadWithContext(b *testing.B) {
	const chunkSize = 64
	cancelable, cancel := context.WithCancel(b.Context())
	defer cancel()
	requestScoped := cancelable
	for depth := range requestKey(10) {
		requestScoped = context.WithValue(requestScoped, depth, depth)
	}
	benchmarks := []struct {
		name string
		opts []ringbuffer.Option
	}{
		{
			name: "no ctx",
		},
		{
			name: "cancel ctx",
			opts: []ringbuffer.Option{ringbuffer.WithContext(cancelable)},
		},
		{
			name: "cancel ctx under 10 values",
			opts: []ringbuffer.Option{ringbuffer.WithContext(requestScoped)},
		},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			rb := ringbuffer.New(64<<10, bm.opts...)
			chunk := make([]byte, chunkSize)
			dest := make([]byte, chunkSize)
			b.SetBytes(chunkSize)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = rb.Write(chunk)
				_, _ = rb.Read(dest)
			}
		})
	}
}

func BenchmarkBlockedHandoff(b *testing.B) {
	benchmarks := []struct {
		name string
		opts []ringbuffer.Option
	}{
		{
			name: "no timeout",
		},
		{
			name: "timeouts",
			opts: []ringbuffer.Option{
				ringbuffer.WithReadTimeout(time.Minute),
				ringbuffer.WithWriteTimeout(time.Minute),
			},
		},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			opts := append([]ringbuffer.Option{
				ringbuffer.WithOverWrite(false),
				ringbuffer.WithReadWaitOnEmpty(true),
			}, bm.opts...)
			rb := ringbuffer.New(1, opts...)
			echoed := ringbuffer.New(1, opts...)
			go func() {
				for {
					c, err := rb.ReadByte()
					if err != nil {
						_ = echoed.Close()
						return
					}
					if err := echoed.WriteByte(c); err != nil {
						return
					}
				}
			}()
			b.ReportAllocs()
			for b.Loop() {
				_ = rb.WriteByte('!')
				_, _ = echoed.ReadByte()
			}
			_ = rb.Close()
		})
	}
}

func BenchmarkParallelWriteRead(b *testing.B) {
	const chunkSize = 256
	rb := ringbuffer.New(64 << 10)
	b.SetBytes(chunkSize)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		chunk := make([]byte, chunkSize)
		dest := make([]byte, chunkSize)
		for pb.Next() {
			_, _ = rb.Write(chunk)
			_, _ = rb.Read(dest)
		}
	})
}
