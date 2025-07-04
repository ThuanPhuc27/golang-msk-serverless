package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ThuanPhuc27/golang-msk-serverless/internal/config"
	"github.com/ThuanPhuc27/golang-msk-serverless/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg, err := config.LoadConfig("../../configs/app.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	client, err := kafka.NewMSKClient([]string{cfg.MSK.Brokers}, cfg.MSK.ConsumerGroup)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Cách subscribe topic trong phiên bản cũ
	err = client.SubscribeTopics([]string{cfg.MSK.Topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe topics: %v", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Consumer started. Waiting for messages...")

	for {
		select {
		case <-sigchan:
			log.Println("Shutting down consumer...")
			return
		default:
			fetches := client.PollFetches(context.Background())
			if fetches.IsClientClosed() {
				log.Println("Client closed")
				return
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				log.Printf(
					"Received message: Topic=%s Partition=%d Offset=%d Key=%s Value=%s\n",
					record.Topic, record.Partition, record.Offset, string(record.Key), string(record.Value),
				)
			}
		}
	}
}