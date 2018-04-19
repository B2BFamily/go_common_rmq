package rmq

import (
	"github.com/streadway/amqp"
)

//объект подключения к очереди
type Connector struct {
	Config *ConfigModel     //настройка очереди
	Conn   *amqp.Connection //Соединение с очередью
	Chan   *amqp.Channel    //канал для работы с очередью
	Que    amqp.Queue       //очередь
	IsInit bool             //флаг, отвечающий за состояние коннектора
}

//модель конфигурации для подключения к очереди
type ConfigQueueModel struct {
	Login    string `json:"login"`    //имя пользователя
	Password string `json:"password"` //пароль
	Server   string `json:"server"`   //сервер, где находится очередь
}

//модель конфигурации очереди
type ConfigModel struct {
	Queue         ConfigQueueModel `json:"queue"`          //настройка для очереди (необходимо, если нет ссылки для подключения к очереди)
	Url           string           `json:"url"`            //ссылка для подключения к очередям (необходимо, если нет настройки для подключения к очереди)
	Name          string           `json:"name"`           //название очереди
	PrefetchCount int              `json:"prefetch_count"` //количество одновременно выполняемых запросов
	Durable       bool             `json:"durable"`        //необходимо ли сохранять очередь на диск
	AutoAck       bool             `json:"auto_ask"`       //флаг auto-ack, ждет ли очередь результат обработки элемента очереди
}

//объект получяемого элемента из очереди
type Item struct {
	delivery amqp.Delivery
	Body     []byte
}
