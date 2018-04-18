package rmq

import (
	"encoding/json"
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

func (sender *Connector) QueueInit() error {
	var err error
	sender.Conn, err = amqp.Dial(sender.Url)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
		return err
	}
	sender.Chan, err = sender.Conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
		return err
	}
	sender.Que, err = sender.Chan.QueueDeclare(
		sender.Name, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
		return err
	}
	sender.IsInit = true
	return nil
}

func (sender *Connector) QueueClose() {
	sender.Chan.Close()
	sender.Conn.Close()
	sender.IsInit = false
}

func (sender *Connector) Send(mess interface{}) {
	_ = sender.Push(mess)
}

func (sender *Connector) Push(mess interface{}) error {
	if !sender.IsInit {
		sender.QueueInit()
		defer sender.QueueClose()
	}

	out, err := json.Marshal(mess)
	if err != nil {
		log.Fatalf("Failed to convert message: %s", err)
		return err
	}

	err = sender.Chan.Publish(
		"",              // exchange
		sender.Que.Name, // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(string(out)),
		})
	if err != nil {
		log.Fatalf("Failed to publish a message: %s", err)
		return err
	}
	return nil
}

func (sender *Connector) Pop(callback func([]byte)) error {
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
	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
		return err
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			callback(d.Body)
		}
	}()

	<-forever
	return nil
}
