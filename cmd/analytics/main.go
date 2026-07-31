package main

import (
	"encoding/json"
	"fmt"
	"log"
	"proiect-rabbitmq/config"
	"proiect-rabbitmq/schema"
)

type Analytics struct {
	TotalOrders    int
	TotalConfirmed int
	TotalFailed    int
	TotalAmount    float64
}

func main() {
	rabbit := config.ConnectRabbitMQ()
	defer rabbit.Close()

	// fanout exchange
	err := rabbit.Channel.ExchangeDeclare("orders_exchange", "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("exchange declaration error: %v", err)
	}

	// queue for analytics
	q, err := rabbit.Channel.QueueDeclare("analytics_queue", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("analytics queue declaration error: %v", err)
	}

	// bind queue to exchange
	err = rabbit.Channel.QueueBind(q.Name, "", "orders_exchange", false, nil)
	if err != nil {
		log.Fatalf("analytics queue bind to exchange error: %v", err)
	}

	// consuming msg from queue
	msgs, err := rabbit.Channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("error consuming from analytics: %v", err)
	}

	stats := Analytics{}
	fmt.Println("[analytics] starting live analytics service")

	for d := range msgs {
		var order schema.Order
		err := json.Unmarshal(d.Body, &order)
		if err != nil {
			continue
		}

		if order.Amount <= 0 || order.Amount > 10000 {
			stats.TotalFailed++
		} else {
			stats.TotalConfirmed++
			stats.TotalAmount += order.Amount
		}

		stats.TotalOrders++

		fmt.Printf("[analytics] live update (%d orders): %d confirmed, %d failed, %.2f RON spent)\n", stats.TotalOrders, stats.TotalConfirmed, stats.TotalFailed, stats.TotalAmount)
	}
}
