package main

import (
	"github.com/B2BFamily/rmq"
	"log"
	"math/rand"
	"time"
)

func sending() {
	sender := rmq.Connector{}
	sender.QueueInit()
	defer sender.QueueClose()
	for i := 0; i < 20; i++ {
		sender.Push(i)
		log.Print(i)
		amt := time.Duration(rand.Intn(300))
		time.Sleep(time.Millisecond * amt)
	}
}

func listening() {
	listner := rmq.Connector{}
	listner.Pop(func(body []byte) {
		log.Printf("Received a message: %s", body)
	})

}

func main() {
	wait := make(chan bool)
	go listening()
	amt := time.Duration(rand.Intn(1000))
	time.Sleep(time.Millisecond * amt)
	sending()
	<-wait
}
