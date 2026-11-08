package ringbuffer_test

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand/v2"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/barnowlsnest/go-ringbufferlib/pkg/ringbuffer"
)

type RingBufferSuite struct {
	suite.Suite
}

func TestRingBufferSuite(t *testing.T) {
	suite.Run(t, new(RingBufferSuite))
}

type op int

const (
	opWrite op = iota
	opRead
	opClose
)

type step struct {
	op      op
	data    string
	readLen int
	want    string
	wantN   int
	wantErr error
}

func do(rb *ringbuffer.RingBuffer, st step) (int, string, error) {
	switch st.op {
	case opWrite:
		n, err := rb.Write([]byte(st.data))
		return n, "", err
	case opRead:
		buf := make([]byte, st.readLen)
		n, err := rb.Read(buf)
		return n, string(buf[:n]), err
	default:
		return 0, "", rb.Close()
	}
}

func (s *RingBufferSuite) TestSequential() {
	tests := []struct {
		name  string
		size  uint64
		opts  []ringbuffer.Option
		steps []step
	}{
		{
			name: "write then read",
			size: 8,
			steps: []step{
				{op: opWrite, data: "abc", wantN: 3},
				{op: opRead, readLen: 8, want: "abc"},
				{op: opRead, readLen: 1, wantErr: ringbuffer.ErrEmpty},
			},
		},
		{
			name: "read from empty buffer",
			size: 8,
			steps: []step{
				{op: opRead, readLen: 4, wantErr: ringbuffer.ErrEmpty},
			},
		},
		{
			name: "zero-length write and read",
			size: 8,
			steps: []step{
				{op: opWrite, data: "", wantN: 0},
				{op: opRead, readLen: 0, want: ""},
			},
		},
		{
			name: "destination larger than capacity",
			size: 4,
			steps: []step{
				{op: opWrite, data: "ab", wantN: 2},
				{op: opRead, readLen: 512, want: "ab"},
			},
		},
		{
			name: "partial reads keep order",
			size: 8,
			steps: []step{
				{op: opWrite, data: "abcdef", wantN: 6},
				{op: opRead, readLen: 2, want: "ab"},
				{op: opRead, readLen: 2, want: "cd"},
				{op: opRead, readLen: 8, want: "ef"},
			},
		},
		{
			name: "wraparound without overwrite",
			size: 4,
			opts: []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			steps: []step{
				{op: opWrite, data: "abc", wantN: 3},
				{op: opRead, readLen: 2, want: "ab"},
				{op: opWrite, data: "def", wantN: 3},
				{op: opRead, readLen: 4, want: "cdef"},
			},
		},
		{
			name: "full cycle reuses buffer",
			size: 4,
			opts: []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			steps: []step{
				{op: opWrite, data: "abcd", wantN: 4},
				{op: opRead, readLen: 4, want: "abcd"},
				{op: opWrite, data: "efgh", wantN: 4},
				{op: opRead, readLen: 4, want: "efgh"},
			},
		},
		{
			name: "overwrite drops oldest",
			size: 4,
			steps: []step{
				{op: opWrite, data: "abcd", wantN: 4},
				{op: opWrite, data: "ef", wantN: 2},
				{op: opRead, readLen: 4, want: "cdef"},
			},
		},
		{
			name: "overwrite when exactly full",
			size: 4,
			steps: []step{
				{op: opWrite, data: "abcd", wantN: 4},
				{op: opWrite, data: "e", wantN: 1},
				{op: opRead, readLen: 4, want: "bcde"},
			},
		},
		{
			name: "overwrite across wrap",
			size: 4,
			steps: []step{
				{op: opWrite, data: "abc", wantN: 3},
				{op: opRead, readLen: 2, want: "ab"},
				{op: opWrite, data: "def", wantN: 3},
				{op: opWrite, data: "gh", wantN: 2},
				{op: opRead, readLen: 4, want: "efgh"},
			},
		},
		{
			name: "oversize write rejected in overwrite mode",
			size: 4,
			steps: []step{
				{op: opWrite, data: "abcdefgh", wantN: 0, wantErr: ringbuffer.ErrTooLargeForWrite},
				{op: opRead, readLen: 1, wantErr: ringbuffer.ErrEmpty},
			},
		},
		{
			name: "size rounds up to power of two",
			size: 6,
			steps: []step{
				{op: opWrite, data: "12345678", wantN: 8},
				{op: opRead, readLen: 8, want: "12345678"},
			},
		},
		{
			name: "size zero uses default",
			size: 0,
			steps: []step{
				{op: opWrite, data: "a", wantN: 1},
				{op: opRead, readLen: 1, want: "a"},
			},
		},
		{
			name: "closed buffer returns rest then EOF",
			size: 8,
			steps: []step{
				{op: opWrite, data: "abc", wantN: 3},
				{op: opClose},
				{op: opRead, readLen: 2, want: "ab"},
				{op: opRead, readLen: 2, want: "c"},
				{op: opRead, readLen: 2, wantErr: io.EOF},
				{op: opClose},
			},
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size, tt.opts...)
			for i, st := range tt.steps {
				n, got, err := do(rb, st)
				s.Require().ErrorIs(err, st.wantErr, "step %d", i)
				switch st.op {
				case opWrite:
					s.Require().Equal(st.wantN, n, "step %d", i)
				case opRead:
					s.Require().Equal(st.want, got, "step %d", i)
				}
			}
		})
	}
}

// synctest.Wait returns once every other goroutine is durably blocked
// (sync.Cond.Wait counts), so "does it block?" needs no sleeps.
func (s *RingBufferSuite) TestBlocking() {
	tests := []struct {
		name    string
		opts    []ringbuffer.Option
		prefill string
		blocked step
		release step
		wantN   int
		wantErr error
	}{
		{
			name:    "read waits for write",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			blocked: step{op: opRead, readLen: 4},
			release: step{op: opWrite, data: "hi"},
			wantN:   2,
		},
		{
			name:    "read released by close",
			opts:    []ringbuffer.Option{ringbuffer.WithReadWaitOnEmpty(true)},
			blocked: step{op: opRead, readLen: 4},
			release: step{op: opClose},
			wantN:   0,
			wantErr: io.EOF,
		},
		{
			name:    "write waits for room",
			opts:    []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			prefill: "abcd",
			blocked: step{op: opWrite, data: "ef"},
			release: step{op: opRead, readLen: 2},
			wantN:   2,
		},
		{
			name:    "write released by close",
			opts:    []ringbuffer.Option{ringbuffer.WithOverWrite(false)},
			prefill: "abcd",
			blocked: step{op: opWrite, data: "ef"},
			release: step{op: opClose},
			wantN:   0,
			wantErr: io.EOF,
		},
		{
			name: "write released by drain",
			opts: []ringbuffer.Option{
				ringbuffer.WithOverWrite(false),
				ringbuffer.WithDrainOnClose(true, time.Second),
			},
			prefill: "abcd",
			blocked: step{op: opWrite, data: "ef"},
			release: step{op: opClose},
			wantN:   0,
			wantErr: ringbuffer.ErrDraining,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				rb := ringbuffer.New(4, tt.opts...)
				if tt.prefill != "" {
					_, err := rb.Write([]byte(tt.prefill))
					require.NoError(t, err)
				}

				type result struct {
					n   int
					err error
				}
				done := make(chan result, 1)
				go func() {
					n, _, err := do(rb, tt.blocked)
					done <- result{n: n, err: err}
				}()

				synctest.Wait()
				require.Empty(t, done, "operation should block")

				_, _, _ = do(rb, tt.release)
				synctest.Wait()
				require.Len(t, done, 1, "operation should be released")

				res := <-done
				require.ErrorIs(t, res.err, tt.wantErr)
				require.Equal(t, tt.wantN, res.n)
			})
		})
	}
}

// Fake clock: the drain timeout elapses instantly and exactly.
func (s *RingBufferSuite) TestDrainOnClose() {
	const timeout = 5 * time.Second
	tests := []struct {
		name        string
		readDuring  bool
		wantElapsed time.Duration
		wantLeft    string
	}{
		{
			name:        "finishes when reader empties buffer",
			readDuring:  true,
			wantElapsed: 0,
			wantLeft:    "",
		},
		{
			name:        "gives up after timeout",
			readDuring:  false,
			wantElapsed: timeout,
			wantLeft:    "abc",
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				rb := ringbuffer.New(8, ringbuffer.WithDrainOnClose(true, timeout))
				_, err := rb.Write([]byte("abc"))
				require.NoError(t, err)

				start := time.Now()
				closed := make(chan struct{})
				go func() {
					_ = rb.Close()
					close(closed)
				}()
				synctest.Wait()

				_, err = rb.Write([]byte("x"))
				require.ErrorIs(t, err, ringbuffer.ErrDraining, "writes rejected while draining")

				if tt.readDuring {
					buf := make([]byte, 8)
					n, err := rb.Read(buf)
					require.NoError(t, err, "reads allowed while draining")
					require.Equal(t, "abc", string(buf[:n]))
				}
				<-closed
				require.Equal(t, tt.wantElapsed, time.Since(start))

				left, err := io.ReadAll(rb)
				require.NoError(t, err)
				require.Equal(t, tt.wantLeft, string(left))
				require.NoError(t, rb.Close(), "Close is idempotent")
			})
		})
	}
}

func (s *RingBufferSuite) TestIOReaderContract() {
	content := []byte("hello, ring buffer")
	rb := ringbuffer.New(64)
	_, err := rb.Write(content)
	s.Require().NoError(err)
	s.Require().NoError(rb.Close())
	s.Require().NoError(iotest.TestReader(rb, content))
}

func (s *RingBufferSuite) TestStreamE2E() {
	src := make([]byte, 128<<10)
	_, _ = rand.NewChaCha8([32]byte{1}).Read(src)
	want := sha256.Sum256(src)

	same := func(r io.Reader) io.Reader { return r }
	tests := []struct {
		name     string
		size     uint64
		producer func(io.Reader) io.Reader
		consumer func(io.Reader) io.Reader
	}{
		{
			// bytes.Reader implements WriterTo, so this is one 128 KiB Write
			name:     "single write larger than buffer",
			size:     4096,
			producer: same,
			consumer: same,
		},
		{
			name:     "one-byte writes",
			size:     64,
			producer: iotest.OneByteReader,
			consumer: same,
		},
		{
			name:     "one-byte reads",
			size:     64,
			producer: same,
			consumer: iotest.OneByteReader,
		},
		{
			name:     "half-size reads and writes, non power of two",
			size:     1000,
			producer: iotest.HalfReader,
			consumer: iotest.HalfReader,
		},
		{
			name:     "capacity of one byte",
			size:     1,
			producer: same,
			consumer: same,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rb := ringbuffer.New(tt.size,
				ringbuffer.WithOverWrite(false),
				ringbuffer.WithReadWaitOnEmpty(true),
			)
			produced := make(chan error, 1)
			go func() {
				_, err := io.Copy(rb, tt.producer(bytes.NewReader(src)))
				_ = rb.Close()
				produced <- err
			}()

			h := sha256.New()
			n, err := io.Copy(h, tt.consumer(rb))
			s.Require().NoError(err)
			s.Require().NoError(<-produced)
			s.Require().Equal(int64(len(src)), n)
			s.Require().Equal(want[:], h.Sum(nil))
		})
	}
}

const (
	fuzzWrite   byte = 0x00
	fuzzRead    byte = 0x80
	fuzzLenMask byte = 0x7f
)

func FuzzOverwriteModel(f *testing.F) {
	seeds := []struct {
		name         string
		capacityLog2 uint8
		payload      []byte
		ops          []byte
	}{
		{
			name:         "writes and reads that wrap and overwrite",
			capacityLog2: 2,
			payload:      []byte("abcdefghijklmnop"),
			ops: []byte{
				fuzzWrite | 3,
				fuzzRead | 2,
				fuzzWrite | 5,
				fuzzRead | 4,
				fuzzWrite | 4,
				fuzzWrite | 2,
				fuzzRead | 8,
			},
		},
		{
			name:         "one-byte capacity",
			capacityLog2: 0,
			payload:      []byte("xyz"),
			ops: []byte{
				fuzzWrite | 1,
				fuzzWrite | 1,
				fuzzRead | 1,
				fuzzWrite | 2,
				fuzzRead | 0,
			},
		},
		{
			name:         "oversize writes rejected",
			capacityLog2: 6,
			payload:      bytes.Repeat([]byte("0123456789"), 20),
			ops: []byte{
				fuzzWrite | 100,
				fuzzRead | 16,
				fuzzWrite | 64,
				fuzzRead | 127,
			},
		},
	}
	for _, seed := range seeds {
		f.Add(seed.capacityLog2, seed.payload, seed.ops)
	}

	f.Fuzz(func(t *testing.T, capacityLog2 uint8, payload, ops []byte) {
		capacity := 1 << (capacityLog2 % 7) // power of two: New must not round it, or the model is wrong
		rb := ringbuffer.New(uint64(capacity))
		var model []byte

		for _, o := range ops {
			length := int(o & fuzzLenMask)
			isRead := o&fuzzRead != 0

			if !isRead {
				length = min(length, len(payload))
				chunk := payload[:length]
				payload = payload[length:]
				written, err := rb.Write(chunk)
				if length > capacity {
					require.ErrorIs(t, err, ringbuffer.ErrTooLargeForWrite)
					require.Zero(t, written)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, length, written)
				model = append(model, chunk...)
				if len(model) > capacity {
					model = model[len(model)-capacity:]
				}
				continue
			}

			dest := make([]byte, length)
			read, err := rb.Read(dest)
			switch {
			case length == 0:
				require.NoError(t, err)
				require.Zero(t, read)
			case len(model) == 0:
				require.ErrorIs(t, err, ringbuffer.ErrEmpty)
			default:
				expected := min(length, len(model))
				require.NoError(t, err)
				require.Equal(t, model[:expected], dest[:read])
				model = model[expected:]
			}
		}
	})
}
