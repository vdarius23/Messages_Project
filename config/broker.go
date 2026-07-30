package config

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	Conn *amqp.Connection
	Channel *amqp.Channel
}

func ConnectRabbitMQ() *Broker {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("RabbitMQ connection error: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel opening error: %v", err)
	}

	return &Broker{
		Conn: conn,
		Channel: ch,
	}
}

func (b *Broker) Close() {
	if b.Channel != nil {
		b.Channel.Close()
	}
	if b.Conn != nil {
		b.Conn.Close()
	}
}