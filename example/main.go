package main

import (
	"github.com/B2BFamily/rmq"
	"log"
	"math/rand"
	"time"
)

func sending() {
	sender := rmq.Connector{
		Url:  "",
		Name: "",
	}
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
	listner := rmq.Connector{
		Url:  "",
		Name: "",
	}
	listner.Pop(func(body []byte) {
		log.Printf("Received a message: %s", body)
	})

}

func main() {
	forever := make(chan bool)
	go sending()
	go listening()
	<-forever
}
