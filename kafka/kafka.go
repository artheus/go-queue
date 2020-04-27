package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"github.com/segmentio/kafka-go"
	"gopkg.in/src-d/go-queue.v1"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	brokerName = "kafka"

	// can safely use characters, as kafka topics can only be named with [a-zA-Z0-9-_] characters
	topicGroupIDSeparator = "@"
	topicPartitionSeparator = ":"

	connectionTimeout = 3 * time.Minute

	maxPortValue = 65535
)

func init() {
	err := envconfig.Process(brokerName, &DefaultConfiguration)
	if err != nil {
		panic(err)
	}

	queue.Register(brokerName, func(uri string) (queue.Broker, error) {
		return New(uri)
	})
}

// Broker implements the queue.Broker interface for AMQP, such as RabbitMQ.
type Broker struct {
	topics    []*Topic
	brokers   []string
	dialer    *kafka.Dialer
}

func (b *Broker) Queue(topicStr string) (_ queue.Queue, err error) {
	var topicName string
	var groupId string
	var partition = 0

	// get group id from topicStr
	if strings.Contains(topicGroupIDSeparator, topicStr) {
		topicParts := strings.Split(topicStr, topicGroupIDSeparator)

		if len(topicParts) != 2 {
			return nil, fmt.Errorf("only one %s character allowed in kafka queue uri. Format: <groupID>@<topic>", topicGroupIDSeparator)
		}

		groupId = topicParts[0]
		topicName = topicParts[1]
	}

	// get partition from topicStr
	if strings.Contains(topicPartitionSeparator, topicStr) {
		topicParts := strings.Split(topicStr, topicPartitionSeparator)

		if len(topicParts) != 2 {
			return nil, fmt.Errorf("only one %s character allowed in kafka queue uri. Format: <topic>:<partition>", topicPartitionSeparator)
		}
	}

	var readerConfig = kafka.ReaderConfig{
		Brokers:   b.brokers,
		GroupID:   groupId,
		Topic:     topicName,
		Partition: partition,
		Dialer:    b.dialer,
	}

	var writerConfig = kafka.WriterConfig{
		Brokers: b.brokers,
		Topic:   "topic",
		Dialer:  b.dialer,
	}

	topicCtx := newTopicCtx(context.WithCancel(context.Background()))

	var topic = &Topic{
		ctx:  topicCtx,
		r:    kafka.NewReader(readerConfig),
		w:    kafka.NewWriter(writerConfig),
		name: topicStr,
	}

	b.topics = append(b.topics, topic)

	return topic, nil
}

func (b *Broker) GetPartitionsForTopic(topic string) (_ []kafka.Partition, err error) {
	ctx, _ := context.WithTimeout(context.Background(), connectionTimeout)

	if topic == "" {
		return nil, errors.New("empty topic name not allowed")
	}

	// pick a kafka broker at random
	randomBroker := b.brokers[rand.Intn(len(b.brokers))]

	return b.dialer.LookupPartitions(ctx, "tcp", randomBroker, topic)
}

func (b *Broker) Close() (err error) {
	for _, t := range b.topics {
		t.Cancel()
	}

	return err
}

// New creates a new Kafka queue.Broker to comma-separated list of kafka brokers
func New(brokers string) (broker queue.Broker, err error) {
	var dialer *kafka.Dialer

	if dialer, err = DefaultConfiguration.Dialer(); err != nil {
		return nil, err
	}

	var brokerSlice = strings.Split(brokers, ",")

	if err = validateBrokers(brokerSlice); err != nil {
		return nil, err
	}

	broker = &Broker{
		brokers: brokerSlice,
		dialer:  dialer,
	}

	return broker, err
}

func validateBrokers(brokers []string) (err error) {
	for _, broker := range brokers {
		brokerParts := strings.Split(broker, ":")

		if len(brokerParts) != 2 {
			return errors.New("broker uri must follow format host:port")
		}

		var port uint64
		if port, err = strconv.ParseUint(brokerParts[1], 10, 64); err != nil {
			return errors.New("broker uri port must be numeric")
		}

		if port == 0 || port > maxPortValue {
			return errors.New("broker uri port must be an int within range 1-65535")
		}

		if _, err = net.LookupHost(brokerParts[0]); err != nil {
			return fmt.Errorf("unable to lookup hostname %s", brokerParts[0])
		}
	}

	return nil
}