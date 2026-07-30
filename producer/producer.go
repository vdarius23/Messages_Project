package main

import (
	"encoding/json"
	"log"
	"proiect-rabbitmq/schema"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Error connecting to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Error creating channel:", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare("orders_dlq", false, false, false, false, nil)
	if err != nil {
		log.Fatal("Error declaring DLQ:", err)
	}

	args := amqp.Table{
		"x-dead-letter-routing-key": "orders_dlq",
		"x-dead-letter-exchange":    "",
	}

	q, err := ch.QueueDeclare("orders_queue", false, false, false, false, args)
	if err != nil {
		log.Fatal("Error declaring orders queue", err)
	}

	orders := []schema.Order{
		{ID: "ORD-101", UserEmail: "ana@gmail.com", Amount: 150.50},
		{ID: "ORD-102", UserEmail: "fraudster@fake.com", Amount: -50.00}, // Invalidă (sumă negativă)
		{ID: "ORD-103", UserEmail: "dan@gmail.com", Amount: 320.00},
		{ID: "ORD-104", UserEmail: "unknown@hacker.org", Amount: 999999.00}, // Suspectă (sumă uriașă)
		{ID: "ORD-105", UserEmail: "elena@yahoo.com", Amount: 85.00},
	}

	for _, order := range orders {
		body, err := json.Marshal(order)
		if err != nil {
			log.Printf("Error serializing JSON %v", err)
			continue
		}

		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
		if err != nil {
			log.Printf("Error publishing order: %v", err)
		} else {
			log.Printf("Order published successfully")
		}
	}

}
