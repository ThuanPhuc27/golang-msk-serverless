package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThuanPhuc27/golang-msk-serverless/internal/config"
	"github.com/ThuanPhuc27/golang-msk-serverless/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg, err := config.LoadConfig("configs/app.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Brokers: %s", cfg.MSK.Brokers)

	client, err := kafka.NewMSKClient([]string{cfg.MSK.Brokers}, "")
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	log.Println("Sending 2 messages to Kafka...")

	for i := 1; i <= 2; i++ {
		record := &kgo.Record{
			Topic: cfg.MSK.Topic,
			Value: []byte(fmt.Sprintf("Test message %d - %s", i, time.Now().Format(time.RFC3339))),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
			log.Printf("Failed to produce message %d: %v", i, err)
			continue
		}

		log.Printf("Sent message %d to topic %s", i, cfg.MSK.Topic)
		time.Sleep(2 * time.Second)
	}

	log.Println("✅ Finished sending 2 messages. Exiting.")
}
