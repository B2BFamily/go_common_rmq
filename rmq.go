package rmq

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Sender struct {
	Url  string
	Name string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func (sender *Sender) Send(mess interface{}) {
	conn, err := amqp.Dial(sender.Url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	q, err := ch.QueueDeclare(
		sender.Name, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")
	out, err := json.Marshal(mess)
	failOnError(err, "Failed to convert message")
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(string(out)),
		})
	failOnError(err, "Failed to publish a message")
}
