package main

import (
        "context"
        "log"
        "os"
        "os/signal"
        "syscall"
        "time"

        "github.com/IBM/sarama"
        "github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

const (
        defaultTopic    = "test-topic"
        defaultMessage  = "Hello MSK!"
        sendInterval    = 5 * time.Second
        shutdownTimeout = 10 * time.Second
)

var (
        kafkaBrokers = []string{"boot-pt9mstca.c2.kafka-serverless.ap-southeast-1.amazonaws.com:9098"}
        awsRegion    = "ap-southeast-1"
)

// MSKAccessTokenProvider handles AWS IAM authentication for MSK
type MSKAccessTokenProvider struct {
        region string
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
        token, expiry, err := signer.GenerateAuthToken(context.TODO(), m.region)
        if err != nil {
                return nil, err
        }
        log.Printf("Obtained new MSK IAM token, expires at: %s", expiry)
        return &sarama.AccessToken{Token: token}, nil
}

// KafkaProducer wraps the async producer with lifecycle management
type KafkaProducer struct {
        producer sarama.AsyncProducer
        done     chan struct{}
}

// NewKafkaProducer creates a new Kafka producer instance
func NewKafkaProducer() (*KafkaProducer, error) {
        config := sarama.NewConfig()
        config.Version = sarama.V2_5_0_0

        // Producer configuration
        config.Producer.RequiredAcks = sarama.WaitForAll
        config.Producer.Retry.Max = 5
        config.Producer.Retry.Backoff = 100 * time.Millisecond
        config.Producer.Return.Successes = true

        // MSK IAM Auth configuration
        config.Net.SASL.Enable = true
        config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
        config.Net.SASL.TokenProvider = &MSKAccessTokenProvider{region: awsRegion}
        config.Net.TLS.Enable = true

        producer, err := sarama.NewAsyncProducer(kafkaBrokers, config)
        if err != nil {
                return nil, err
        }

        kp := &KafkaProducer{
                producer: producer,
                done:     make(chan struct{}),
        }

        // Start monitoring goroutines
        go kp.monitorErrors()
        go kp.monitorSuccesses()

        return kp, nil
}

// monitorErrors handles error messages from the producer
func (kp *KafkaProducer) monitorErrors() {
        for {
                select {
                case err := <-kp.producer.Errors():
                        if err != nil {
                                log.Printf("Failed to produce message: %v", err)
                        }
                case <-kp.done:
                        return
                }
        }
}

// monitorSuccesses handles successful delivery notifications
func (kp *KafkaProducer) monitorSuccesses() {
        for {
                select {
                case success := <-kp.producer.Successes():
                        log.Printf("Message delivered to topic %s (partition %d @ offset %d)",
                                success.Topic, success.Partition, success.Offset)
                case <-kp.done:
                        return
                }
        }
}

// Close shuts down the producer gracefully
func (kp *KafkaProducer) Close() error {
        close(kp.done)
        if err := kp.producer.Close(); err != nil {
                log.Printf("Failed to close producer: %v", err)
                return err
        }
        return nil
}

// ProduceMessage sends a message to Kafka
func (kp *KafkaProducer) ProduceMessage(topic string, key, value []byte) {
        msg := &sarama.ProducerMessage{
                Topic:     topic,
                Key:       sarama.ByteEncoder(key),
                Value:     sarama.ByteEncoder(value),
                Timestamp: time.Now(),
        }

        kp.producer.Input() <- msg
}

func main() {
        log.Println("Starting Kafka producer...")

        // Initialize producer
        producer, err := NewKafkaProducer()
        if err != nil {
                log.Fatalf("Failed to create producer: %v", err)
        }
        defer producer.Close()

        // Set up signal handling for graceful shutdown
        sigchan := make(chan os.Signal, 1)
        signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

        // Message production loop
        ticker := time.NewTicker(sendInterval)
        defer ticker.Stop()

        log.Printf("Producing messages to topic '%s' every %v", defaultTopic, sendInterval)

        for {
                select {
                case <-ticker.C:
                        producer.ProduceMessage(defaultTopic, nil, []byte(defaultMessage))
                case sig := <-sigchan:
                        log.Printf("Received signal %v - shutting down...", sig)
                        return
                }
        }
}