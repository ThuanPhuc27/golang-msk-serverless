package kafka

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
)

func NewMSKClient(brokers []string, groupID string) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		// Cấu hình SASL cho MSK IAM
		kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
			return aws.Auth{}, nil
		})),
		// Bật tính năng SSL
		kgo.DialTLS(),
	}

	if groupID != "" {
		opts = append(opts, kgo.ConsumerGroup(groupID))
	}

	return kgo.NewClient(opts...)
}