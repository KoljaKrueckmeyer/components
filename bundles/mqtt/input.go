package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/wombatwisdom/components/framework/spec"
)

type InputConfig struct {
	CommonMQTTConfig

	// Filters is a map of topics and QoS levels to subscribe to
	Filters map[string]byte

	// CleanSession
	CleanSession bool

	// ClientId is an optional unique identifier for the client
	ClientId string

	// EnableAutoAck enables automatic acknowledgment for at-least-once delivery (paho SetAutoAckDisabled)
	EnableAutoAck bool

	// SessionStabilizationDelay is an optional delay after connection before subscribing
	// This helps ensure broker-side session state is fully initialized (useful for Apache Artemis)
	SessionStabilizationDelay *time.Duration
}

func NewInput(env spec.Environment, config InputConfig) (*Input, error) {
	return &Input{
		InputConfig: config,
		log:         env,
	}, nil
}

type Input struct {
	InputConfig

	client mqtt.Client

	msgChan     chan mqtt.Message
	msgChanLock sync.Mutex
	msgMut      sync.Mutex

	subscribed     bool
	subscribedLock sync.Mutex

	log spec.Logger
}

func (m *Input) closeMsgChan() bool {
	m.msgChanLock.Lock()
	defer m.msgChanLock.Unlock()

	chanOpen := m.msgChan != nil
	if chanOpen {
		close(m.msgChan)
		m.msgChan = nil
	}
	return chanOpen
}

func (m *Input) Init(ctx spec.ComponentContext) error {
	if m.client != nil {
		return spec.ErrAlreadyConnected
	}

	m.msgChan = make(chan mqtt.Message)

	opts := NewClientOptions(m.InputConfig.CommonMQTTConfig).
		SetCleanSession(m.CleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			m.log.Errorf("Connection lost due to: %v\n", reason)
			// Mark as unsubscribed so reconnection handler will re-subscribe
			m.subscribedLock.Lock()
			m.subscribed = false
			m.subscribedLock.Unlock()
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			m.log.Infof("Connected to MQTT broker")

			// For reconnections, re-subscribe after connection is established
			// Only re-subscribe if we were previously subscribed (connection was lost)
			m.subscribedLock.Lock()
			wasSubscribed := m.subscribed
			m.subscribedLock.Unlock()

			if !wasSubscribed {
				// First connection - subscription will be done in Init() after stabilization delay
				return
			}

			// This is a reconnection
			// With persistent sessions (clean_session=false), subscriptions survive
			// across reconnects. Only re-subscribe if clean_session=true
			if m.CleanSession {
				m.log.Infof("Reconnected with clean session - re-subscribing to topics")

				// Apply stabilization delay for reconnections too if configured
				if m.SessionStabilizationDelay != nil && *m.SessionStabilizationDelay > 0 {
					m.log.Infof("Waiting %v for broker session stabilization before re-subscribing", *m.SessionStabilizationDelay)
					time.Sleep(*m.SessionStabilizationDelay)
				}

				if err := m.subscribe(client, ctx); err != nil {
					m.log.Errorf("Failed to re-subscribe after reconnection: %v", err)
				}
			} else {
				m.log.Infof("Reconnected with persistent session - subscriptions already exist on broker, skipping re-subscribe")
				// Mark as subscribed again since the broker still has our subscriptions
				m.subscribedLock.Lock()
				m.subscribed = true
				m.subscribedLock.Unlock()
			}
		}).
		SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
			m.log.Infof("Reconnecting to MQTT broker...")
		}).
		SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
			m.log.Infof("Attempting to reconnect to MQTT broker at %s", broker)
			return tlsCfg
		}).
		SetAutoAckDisabled(!m.EnableAutoAck)

	client := mqtt.NewClient(opts)

	// Connect and wait for completion
	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	m.client = client

	// Apply session stabilization delay if configured
	// This ensures broker-side session state is fully initialized before subscribing
	if m.SessionStabilizationDelay != nil && *m.SessionStabilizationDelay > 0 {
		m.log.Infof("Waiting %v for broker session stabilization before subscribing", *m.SessionStabilizationDelay)
		select {
		case <-time.After(*m.SessionStabilizationDelay):
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}

	// Perform initial subscription after connection is fully established
	if err := m.subscribe(client, ctx); err != nil {
		client.Disconnect(0)
		return err
	}

	go func() {
		for range ctx.Context().Done() {
		}
	}()

	return nil
}

// subscribe performs the actual subscription to configured topics
func (m *Input) subscribe(client mqtt.Client, ctx spec.ComponentContext) error {
	// Build topic list for logging
	topics := make([]string, 0, len(m.Filters))
	for topic, qos := range m.Filters {
		topics = append(topics, topic)
		m.log.Debugf("  - Topic: %s, QoS: %d", topic, qos)
	}
	m.log.Infof("Subscribing to %d topic(s): %v", len(topics), topics)

	tok := client.SubscribeMultiple(m.Filters, func(_ mqtt.Client, msg mqtt.Message) {
		m.msgMut.Lock()
		defer m.msgMut.Unlock()

		m.msgChanLock.Lock()
		msgChan := m.msgChan
		m.msgChanLock.Unlock()

		if msgChan != nil {
			select {
			case msgChan <- msg:
			case <-ctx.Context().Done():
			}
		}
	})

	// Wait for subscription with timeout
	m.log.Debugf("Waiting for subscription acknowledgment...")
	done := make(chan struct{})
	go func() {
		tok.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Subscription completed
		if err := tok.Error(); err != nil {
			m.log.Errorf("Failed to subscribe to topics %v: %v", topics, err)
			m.closeMsgChan()
			return err
		}
	case <-time.After(30 * time.Second):
		m.log.Errorf("Subscription timeout after 30 seconds for topics %v", topics)
		m.closeMsgChan()
		return errors.New("subscription timeout after 30 seconds")
	case <-ctx.Context().Done():
		m.log.Warnf("Subscription cancelled due to context cancellation")
		return ctx.Context().Err()
	}

	m.subscribedLock.Lock()
	m.subscribed = true
	m.subscribedLock.Unlock()

	m.log.Infof("Successfully subscribed to %d topic(s)", len(topics))
	return nil
}

func (m *Input) Close(ctx spec.ComponentContext) error {
	m.msgChanLock.Lock()
	defer m.msgChanLock.Unlock()

	if m.client != nil {
		m.client.Disconnect(0)
		m.client = nil
	}

	return nil
}

func (m *Input) Read(ctx spec.ComponentContext) (spec.Batch, spec.ProcessedCallback, error) {
	m.msgChanLock.Lock()
	msgChan := m.msgChan
	m.msgChanLock.Unlock()

	if msgChan == nil {
		return nil, nil, spec.ErrNotConnected
	}

	select {
	case msg, open := <-msgChan:
		if !open {
			m.closeMsgChan()
			return nil, nil, spec.ErrNotConnected
		}

		specMsg := ctx.NewMessage()
		specMsg.SetRaw(msg.Payload())

		specMsg.SetMetadata("mqtt_duplicate", msg.Duplicate())
		specMsg.SetMetadata("mqtt_qos", int(msg.Qos()))
		specMsg.SetMetadata("mqtt_retained", msg.Retained())
		specMsg.SetMetadata("mqtt_topic", msg.Topic())
		specMsg.SetMetadata("mqtt_message_id", int(msg.MessageID()))

		return ctx.NewBatch(specMsg), func(ackCtx context.Context, res error) error {
			// check for any errors in the component context
			if err := ackCtx.Err(); err != nil {
				if !m.EnableAutoAck {
					var reason string
					switch {
					case errors.Is(err, context.Canceled):
						reason = "context cancellation"
					case errors.Is(err, context.DeadlineExceeded):
						reason = "deadline exceeded"
					default:
						reason = "context error: " + err.Error()
					}
					m.log.Infof("Skipping ACK for message (topic: %s, id: %d) due to %s - message will be redelivered",
						msg.Topic(), msg.MessageID(), reason)
				}
				return nil
			}

			if res == nil {
				if !m.EnableAutoAck {
					// Check if client is still connected before ACKing
					if m.client != nil && m.client.IsConnected() {
						msg.Ack()
					} else {
						m.log.Infof("Skipping ACK for message (topic: %s, id: %d) - client disconnected, message will be redelivered",
							msg.Topic(), msg.MessageID())
					}
				}
			}
			return nil
		}, nil
	case <-ctx.Context().Done():
		return nil, nil, ctx.Context().Err()
	}
}
