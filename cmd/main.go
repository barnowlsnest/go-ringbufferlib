package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
	"time"
	
	"github.com/barnowlsnest/go-ringbufferlib/pkg/ringbuffer"
)

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

func Rand8() (string, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	for i := range b {
		b[i] = alphabet[b[i]&63]
	}
	return string(b), nil
}

func main() {
	var wg sync.WaitGroup
	r := ringbuffer.New(64,
		ringbuffer.WithWriteWaitOnFull(true),
	)
	
	defer func() { _ = r.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	wg.Go(func() {
		t := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
				s, err := Rand8()
				if err != nil {
					fmt.Println("write err:", err)
				}
				n, err := r.Write([]byte(s))
				if err != nil {
					fmt.Println("write err:", err)
					return
				}
				fmt.Println("write", n, s)
			}
		}
		
	})
	wg.Go(func() {
		<-time.After(time.Second * 3)
		t := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
				b := make([]byte, 8)
				n, err := r.Read(b)
				if err != nil {
					fmt.Println("read error:", err)
					return
				}
				fmt.Println("read", n, string(b))
			}
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
		stop()
	}()
	<-done
}
