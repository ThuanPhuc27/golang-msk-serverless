package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam"
)

const (
	topic          = "test-topic"
	broker         = "boot-pt9mstca.c2.kafka-serverless.ap-southeast-1.amazonaws.com:9098"
	awsRegion      = "ap-southeast-1"
	consumerGroup  = "test-consumer-group"
	sessionValidity = 15 * time.Minute
)

func main() {
	// 1. Tạo IAM signer
	iamSigner, err := signer.NewDefaultSigner(sessionValidity)
	if err != nil {
		log.Fatalf("Failed to create IAM signer: %v", err)
	}

	// 2. Cấu hình Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: consumerGroup,
		Dialer: &kafka.Dialer{
			SASLMechanism: aws_msk_iam.NewMechanism(iamSigner, awsRegion),
		},
	})

	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Failed to close reader: %v", err)
		}
	}()

	// 3. Xử lý tín hiệu dừng
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Consumer started. Press Ctrl+C to stop...")

	// 4. Nhận message
	for {
		select {
		case <-sigchan:
			log.Println("Shutting down consumer...")
			return
		default:
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading message: %v", err)
				continue
			}

			log.Printf(
				"Received message: Topic=%s Partition=%d Offset=%d Key=%s Value=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value),
			)
		}
	}
}