package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

const (
	consumerGroupID = "test-consumer-group"
)

var (
	kafkaBrokers = []string{"boot-pt9mstca.c2.kafka-serverless.ap-southeast-1.amazonaws.com:9098"}
	awsRegion    = "ap-southeast-1"
	topic        = "test-topic"
)

type MSKAccessTokenProvider struct{}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, expiry, err := signer.GenerateAuthToken(context.TODO(), awsRegion)
	if err != nil {
		return nil, err
	}
	log.Printf("Refreshed MSK IAM token, valid until: %v", expiry)
	return &sarama.AccessToken{Token: token}, nil
}

type Consumer struct {
	ready    chan bool
	msgCount int
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		consumer.msgCount++
		log.Printf("[Message #%d] Topic: %s | Partition: %d | Offset: %d | Key: %s | Value: %s",
			consumer.msgCount,
			message.Topic,
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value))
		session.MarkMessage(message, "")
	}
	return nil
}

func createConsumer() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	config.Net.SASL.TokenProvider = &MSKAccessTokenProvider{}
	config.Net.TLS.Enable = true
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(kafkaBrokers, consumerGroupID, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func main() {
	log.Println("Starting MSK Consumer for topic:", topic)
	log.Println("Brokers:", kafkaBrokers)
	log.Println("Region:", awsRegion)
	log.Println("Consumer Group:", consumerGroupID)

	consumer, err := createConsumer()
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	handler := &Consumer{
		ready: make(chan bool),
	}

	go func() {
		defer wg.Done()
		for {
			if err := consumer.Consume(ctx, []string{topic}, handler); err != nil {
				log.Printf("Consume error: %v", err)
				time.Sleep(5 * time.Second)
			}
			if ctx.Err() != nil {
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	<-handler.ready
	log.Println("Consumer successfully initialized and running")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigterm:
		log.Println("Received shutdown signal, initiating graceful shutdown...")
	case <-ctx.Done():
		log.Println("Context cancelled")
	}
	cancel()
	wg.Wait()
	log.Println("Consumer shutdown completed")
}