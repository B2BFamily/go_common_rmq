package rmq

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Connector struct {
	Url    string           //ссылка для подключения к очередям
	Name   string           //название очереди
	Conn   *amqp.Connection //Соединение с очередью
	Chan   *amqp.Channel    //канал для работы с очередью
	Que    amqp.Queue       //очередь
	IsInit bool             //флаг, отвечающий за состояние коннектора
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func (sender *Connector) QueueInit() {
	var err error
	sender.Conn, err = amqp.Dial(sender.Url)
	failOnError(err, "Failed to connect to RabbitMQ")
	sender.Chan, err = sender.Conn.Channel()
	failOnError(err, "Failed to open a channel")
	sender.Que, err = sender.Chan.QueueDeclare(
		sender.Name, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")
	sender.IsInit = true
}

func (sender *Connector) QueueClose() {
	sender.Chan.Close()
	sender.Conn.Close()
	sender.IsInit = false
}

func (sender *Connector) Push(mess interface{}) {
	if !sender.IsInit {
		sender.QueueInit()
		defer sender.QueueClose()
	}

	out, err := json.Marshal(mess)
	failOnError(err, "Failed to convert message")

	err = sender.Chan.Publish(
		"",              // exchange
		sender.Que.Name, // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(string(out)),
		})
	failOnError(err, "Failed to publish a message")
}

func (sender *Connector) Pop(callback func([]byte)) {
	if !sender.IsInit {
		sender.QueueInit()
		defer sender.QueueClose()
	}

	msgs, err := sender.Chan.Consume(
		sender.Que.Name, // queue
		"",              // consumer
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			// log.Printf("Received a message: %s", d.Body)
			callback(d.Body)
		}
	}()

	// log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
