package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Order struct {
	ID        string  `json:"id" bson:"_id,omitempty"`
	UserEmail string  `json:"user_email" bson:"user_email"`
	Amount    float64 `json:"amount" bson:"amount"`
	Status    string  `json:"status" bson:"status"`
}

func orderProcess(d amqp.Delivery) {
	var order Order

	err := json.Unmarshal(d.Body, &order)
	if err != nil {
		fmt.Println(" JSON error")
		d.Nack(false, false)
		return
	}

	fmt.Printf("[order] processing order %s for %s\n", order.ID, order.UserEmail)
	time.Sleep(1 * time.Second)

	if order.Amount <= 0 || order.Amount > 10000 {
		fmt.Printf("[order] order %s rejected (%.2f RON)", order.ID, order.Amount)
		d.Nack(false, false)
	} else {
		order.Status = "PROCESSED"
		fmt.Printf("[order] order sent %s with status: %s", order.ID, order.Status)
		d.Ack(false)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("RabbitMQ connection error: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel opening error: %v", err)
	}
	defer ch.Close()

	args := amqp.Table{
		"x-dead-letter-routing-key": "orders_dlq",
		"x-dead-letter-exchange":    "",
	}

	q, err := ch.QueueDeclare("orders_queue", false, false, false, false, args)
	if err != nil {
		log.Fatalf("error declaring orders queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume error: %v", err)
	}

	fmt.Println("[order] getting orders")
	for d := range msgs {
		go orderProcess(d)
	}
}
