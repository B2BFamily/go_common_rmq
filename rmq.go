//Библиотека для работы с очередями RabbitMQ
package go_common_rmq

import (
	"encoding/json"
	"fmt"
	config "github.com/B2BFamily/go_common_config"
	"github.com/streadway/amqp"
	"log"
)

//На основе конфигурации создаем подключение к очереди
//
//Пример корфигурации для автоматического создания подключения:
//	{
//	  "example": {
//	    "queue": {
//	      "login": "username",
//	      "password": "password",
//	      "server": "server"
//	    },
//	    "url": "amqp://username:password@server/",
//	    "name": "webhooks_for_result_queue_name_test",
//	    "prefetch_count": 2,
//	    "durable": true
//	    "auto_ask": true
//	  }
//	}
//
//Пример вызова
//	conn := Create("example")
func Create(configPath string) *Connector {
	conn := new(Connector)
	config.GetConfigPath(configPath, &conn.Config)
	if len(conn.Config.Url) == 0 {
		conn.Config.Url = fmt.Sprintf("amqp://%v:%v@%v/", conn.Config.Queue.Login, conn.Config.Queue.Password, conn.Config.Queue.Server)
	}
	return conn
}

func CreateWithConfigModel(config *ConfigModel) *Connector {
	conn := new(Connector)
	conn.Config = *config
	if len(conn.Config.Url) == 0 {
		conn.Config.Url = fmt.Sprintf("amqp://%v:%v@%v/", conn.Config.Queue.Login, conn.Config.Queue.Password, conn.Config.Queue.Server)
	}
	return conn
}

//Инициализация очереди
func (sender *Connector) QueueInit() error {
	var err error
	sender.Conn, err = amqp.Dial(sender.Config.Url)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
		return err
	}
	sender.Chan, err = sender.Conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
		return err
	}
	if sender.Config.PrefetchCount > 0 {
		err = sender.Chan.Qos(sender.Config.PrefetchCount, 0, false)
		if err != nil {
			log.Fatalf("Failed to set QoS: %s", err)
			return err
		}
	}
	sender.Que, err = sender.Chan.QueueDeclare(
		sender.Config.Name,    // name
		sender.Config.Durable, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
		return err
	}
	sender.IsInit = true
	return nil
}

//Отключение от очереди
func (sender *Connector) QueueClose() {
	sender.Chan.Close()
	sender.Conn.Close()
	sender.IsInit = false
}

//Отправка сообщения в очередь (устаревший)
func (sender *Connector) Send(mess interface{}) {
	_ = sender.Push(mess)
}

//Отправка сообщения в очередь
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

//Отправка сообщения в очередь
func (sender *Connector) PushString(mess string) error {
	if !sender.IsInit {
		sender.QueueInit()
		defer sender.QueueClose()
	}

	err := sender.Chan.Publish(
		"",              // exchange
		sender.Que.Name, // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(mess),
		})
	if err != nil {
		log.Fatalf("Failed to publish a message: %s", err)
		return err
	}
	return nil
}

//Подключение к очереди для её прослушивания
func (sender *Connector) Pop(callback func(*Item)) error {
	if !sender.IsInit {
		sender.QueueInit()
		defer sender.QueueClose()
	}

	msgs, err := sender.Chan.Consume(
		sender.Que.Name, // queue
		"",              // consumer
		sender.Config.AutoAck, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
		return err
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			callback(createItem(d))
		}
	}()

	<-forever
	return nil
}
