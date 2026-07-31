package main

import (
	"encoding/json"
	"fmt"
	"log"
	"proiect-rabbitmq/config"
	"proiect-rabbitmq/schema"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func orderProcess(d amqp.Delivery) {
	var order schema.Order

	err := json.Unmarshal(d.Body, &order)
	if err != nil {
		fmt.Println(" JSON error")
		d.Nack(false, false)
		return
	}

	fmt.Printf("[order] processing order %s for %s\n", order.ID, order.UserEmail)
	time.Sleep(1 * time.Second)

	if order.Amount <= 0 || order.Amount > 10000 {
		fmt.Printf("[order] order %s rejected (%.2f RON)\n", order.ID, order.Amount)
		d.Nack(false, false)
	} else {
		order.Status = "PROCESSED"
		fmt.Printf("[order] order sent %s with status: %s\n", order.ID, order.Status)
		d.Ack(false)
	}
}

func main() {
	rabbit := config.ConnectRabbitMQ()
	defer rabbit.Close()

	err := rabbit.Channel.ExchangeDeclare("orders_exchange", "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("exchange declaration error: %v", err)
	}

	_, err = rabbit.Channel.QueueDeclare("orders_dlq", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("dlq declaration error: %v", err)
	}

	args := amqp.Table{
		"x-dead-letter-routing-key": "orders_dlq",
		"x-dead-letter-exchange":    "",
	}

	q, err := rabbit.Channel.QueueDeclare("consumer1_queue", false, false, false, false, args)
	if err != nil {
		log.Fatalf("error declaring orders queue: %v", err)
	}

	err = rabbit.Channel.QueueBind(q.Name, "", "orders_exchange", false, nil)
	if err != nil {
		log.Fatalf("queue bind to exchange error: %v", err)
	}

	msgs, err := rabbit.Channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume error: %v", err)
	}

	fmt.Println("[order] getting orders")
	for d := range msgs {
		go orderProcess(d)
	}
}
