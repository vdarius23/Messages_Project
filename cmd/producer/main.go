package main

import (
	"encoding/json"
	"log"
	"proiect-rabbitmq/config"
	"proiect-rabbitmq/schema"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbit := config.ConnectRabbitMQ()
	defer rabbit.Close()

	err := rabbit.Channel.ExchangeDeclare("orders_exchange", "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error declaring exchange : %v", err)
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

		err = rabbit.Channel.Publish(
			"orders_exchange", // Trimitem la Exchange-ul Fanout
			"",                // RoutingKey e ignorat de Fanout
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		if err != nil {
			log.Printf("Error publishing order: %v", err)
		} else {
			log.Printf("Order published successfully")
		}
	}

}
