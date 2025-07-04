package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam"
)

const (
	topic          = "test-topic"
	broker         = "boot-pt9mstca.c2.kafka-serverless.ap-southeast-1.amazonaws.com:9098" // Thay bằng endpoint thực tế
	awsRegion      = "ap-southeast-1"                 // Thay bằng region của bạn
	sessionValidity = 15 * time.Minute                // Thời hạn token IAM
)

func main() {
	// 1. Tạo IAM signer
	iamSigner, err := signer.NewDefaultSigner(sessionValidity)
	if err != nil {
		log.Fatalf("Failed to create IAM signer: %v", err)
	}

	// 2. Cấu hình SASL mechanism
	mechanism := aws_msk_iam.NewMechanism(iamSigner, awsRegion)

	// 3. Tạo Kafka writer với IAM authentication
	writer := &kafka.Writer{
		Addr:      kafka.TCP(broker),
		Topic:     topic,
		Transport: &kafka.Transport{SASL: mechanism},
		Async:     false, // Đồng bộ để kiểm tra lỗi ngay lập tức
	}

	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Failed to close writer: %v", err)
		}
	}()

	// 4. Gửi message
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("Message %d - %s", i, time.Now().Format(time.RFC3339))
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(msg),
			},
		)

		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
			continue
		}

		log.Printf("Sent message %d: %s", i, msg)
		time.Sleep(2 * time.Second)
	}
}