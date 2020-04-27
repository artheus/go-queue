package kafka

import (
	"crypto/tls"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"net"
	"strings"
	"time"
)

// DefaultConfiguration contains the default configuration initialized from
// environment variables.
var DefaultConfiguration Configuration

// Configuration Kafka configuration settings, this settings are set using the
// environment variables.
type Configuration struct {
	// w/o default value
	ClientID        string    `envconfig:"KAFKA_CLIENT_ID"`
	LocalIP         string    `envconfig:"KAFKA_LOCAL_IP"`
	Deadline        time.Time `envconfig:"KAFKA_CONNECTION_DEADLINE"`
	SASLMechanism   string    `envconfig:"KAFKA_SASL_MECHANISM"`
	SASLUsername    string    `envconfig:"KAFKA_SASL_USERNAME"`
	SASLPassword    string    `envconfig:"KAFKA_SASL_PASSWORD"`
	TransactionalID string    `envconfig:"KAFKA_TRANSACTIONAL_ID"`

	// with default value
	DualStack     bool   `envconfig:"KAFKA_DUAL_STACK" default:"false"`
	TlsEnabled    bool   `envconfig:"KAFKA_TLS_ENABLED" default:"true"`
	TlsSkipVerify bool   `envconfig:"KAFKA_TLS_INSECURE_SKIP_VERIFY" default:"false"`
	Timeout       string `envconfig:"KAFKA_CONNECTION_TIMEOUT" default:"3m"`
	FallbackDelay string `envconfig:"KAFKA_FALLBACK_DELAY" default:"300ms"`
	KeepAlive     string `envconfig:"KAFKA_KEEP_ALIVE" default:"0"`
}

func (c Configuration) Dialer() (dialer *kafka.Dialer, err error) {
	var timeout time.Duration
	if timeout, err = time.ParseDuration(DefaultConfiguration.Timeout); err != nil {
		return nil, err
	}

	var fallbackDelay time.Duration
	if fallbackDelay, err = time.ParseDuration(DefaultConfiguration.FallbackDelay); err != nil {
		return nil, err
	}

	var keepAlive time.Duration
	if keepAlive, err = time.ParseDuration(DefaultConfiguration.KeepAlive); err != nil {
		return nil, err
	}

	var localAddr net.Addr
	if DefaultConfiguration.LocalIP != "" {
		if localAddr, err = net.ResolveIPAddr("ip", DefaultConfiguration.LocalIP); err != nil {
			panic(err)
		}
	}

	var tlsConfig *tls.Config
	if DefaultConfiguration.TlsEnabled {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: DefaultConfiguration.TlsSkipVerify,
		}
	}

	var saslMechanism sasl.Mechanism
	switch strings.ToUpper(DefaultConfiguration.SASLMechanism) {
	case "PLAIN":
		saslMechanism = plain.Mechanism{
			Username: DefaultConfiguration.SASLUsername,
			Password: DefaultConfiguration.SASLPassword,
		}
		break
	case "SCRAM-SHA-256":
		if saslMechanism, err = scram.Mechanism(
			scram.SHA256,
			DefaultConfiguration.SASLUsername,
			DefaultConfiguration.SASLPassword,
		); err != nil {
			panic(err)
		}
		break
	case "SCRAM-SHA-512":
		if saslMechanism, err = scram.Mechanism(
			scram.SHA256,
			DefaultConfiguration.SASLUsername,
			DefaultConfiguration.SASLPassword,
		); err != nil {
			return nil, err
		}
		break
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", DefaultConfiguration.SASLMechanism)
	}

	dialer = &kafka.Dialer{
		ClientID:        DefaultConfiguration.ClientID,
		Timeout:         timeout,
		Deadline:        time.Time{},
		LocalAddr:       localAddr,
		DualStack:       DefaultConfiguration.DualStack,
		FallbackDelay:   fallbackDelay,
		KeepAlive:       keepAlive,
		TLS:             tlsConfig,
		SASLMechanism:   saslMechanism,
		TransactionalID: DefaultConfiguration.TransactionalID,
	}

	return dialer, nil
}
