package ringbuffer_test

import (
	"io"
	"testing"

	"github.com/barnowlsnest/go-ringbufferlib/pkg/ringbuffer"
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
