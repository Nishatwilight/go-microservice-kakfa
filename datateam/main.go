package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "HVSE"
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo-dataTeam",
		"auto.offset.reset": "smallest"})
	fmt.Printf("consumer value %v\n", consumer)

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatal(err)
	}
	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("DataTeam reading the  Order: %s\n", string(e.Value))
		case *kafka.Error:
			fmt.Printf("error value %v\n", e)
		}
	}
}
