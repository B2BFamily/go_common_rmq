package rmq

import (
	"github.com/streadway/amqp"
)

//Ответ о завершении обработки элемента в очеред, необходим если в очереди
func (item Item) Ack(multiple bool) error {
	return item.delivery.Ack(multiple)
}

func createItem(d amqp.Delivery) *Item {
	item := new(Item)
	item.delivery = d
	item.Body = d.Body
	return item
}
