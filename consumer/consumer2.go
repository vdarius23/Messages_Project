package main

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FraudLog struct {
	ID        string  `json:"id" bson:"order_id"`
	UserEmail string  `json:"user_email" bson:"user_email"`
	Amount    float64 `json:"amount" bson:"amount"`
	Reason    string  `json:"reason" bson:"reason"`
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

	q, err := ch.QueueDeclare("orders_dlq", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("dlq declaration error: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("error from dlq queue: %v", err)
	}

	fmt.Println("[validation] getting msg from dlq")

	for d := range msgs {
		var logData FraudLog
		json.Unmarshal(d.Body, &logData)
		logData.Reason = "SUSPICIOUS_AMOUNT_OR_INVALID"
		fmt.Printf("[validation] invalid order %s from %s (%.2f RON)", logData.ID, logData.UserEmail, logData.Amount)
	}
}
