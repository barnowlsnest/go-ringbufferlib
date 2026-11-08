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

func Rand() (string, error) {
	b := make([]byte, 96*1024)
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
	r := ringbuffer.New(96000,
		ringbuffer.WithDrainOnClose(true, time.Second*10),
		ringbuffer.WithReadWaitOnEmpty(true),
	)
	defer func() { _ = r.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() { <-ctx.Done(); _ = r.Close() }()

	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				s, err := Rand()
				if err != nil {
					fmt.Println(err)
					return
				}
				_, err = r.Write([]byte(s))
				if err != nil {
					fmt.Println("write err:", err)
					return
				}
			}
			<-time.After(12 * time.Millisecond)
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				b := make([]byte, 92*1024)
				n, err := r.Read(b)
				if err != nil {
					fmt.Println("read err:", err)
					return
				}
				fmt.Println("read:", n, string(b[:n]))
			}
			<-time.After(time.Second)
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
