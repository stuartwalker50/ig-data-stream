// Package publisher handles the ZeroMQ transport layer.
//
// Outbound (PUB socket) – publishes normalised messages for prices, account
// updates and trade events.
//
// Inbound (SUB socket) – subscribes to order commands sent by strategy
// services; messages are routed to a channel consumed by the order executor.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-zeromq/zmq4"
)

// Topic constants used as ZeroMQ message frame prefixes.
const (
	TopicPrices  = "prices"
	TopicAccount = "account"
	TopicTrades  = "trades"
)

// Envelope is the normalised message wrapper published on PUB sockets.
type Envelope struct {
	Type        string          `json:"type"`
	AccountMode string          `json:"account_mode"`
	AccountID   string          `json:"account_id"`
	Epic        string          `json:"epic,omitempty"`
	Ts          string          `json:"ts"`
	Payload     json.RawMessage `json:"payload"`
}

// OrderCommand is the message structure received on the SUB (order command)
// socket from strategy services.
type OrderCommand struct {
	Epic           string  `json:"epic"`
	Direction      string  `json:"direction"`
	Size           float64 `json:"size"`
	OrderType      string  `json:"order_type"`
	CurrencyCode   string  `json:"currency_code"`
	Expiry         string  `json:"expiry"`
	ForceOpen      bool    `json:"force_open"`
	GuaranteedStop bool    `json:"guaranteed_stop"`
	DealReference  string  `json:"deal_reference,omitempty"`
}

// Publisher manages ZeroMQ publish and subscribe sockets.
type Publisher struct {
	pubSock zmq4.Socket
	subSock zmq4.Socket

	accountMode string
	accountID   string
}

// New creates and binds a PUB socket and a SUB socket that listens for order
// commands.
func New(ctx context.Context, pubAddr, subAddr, accountMode, accountID string) (*Publisher, error) {
	pub := zmq4.NewPub(ctx)
	if err := pub.Listen(pubAddr); err != nil {
		pub.Close()
		return nil, fmt.Errorf("zmq pub listen %s: %w", pubAddr, err)
	}

	sub := zmq4.NewSub(ctx)
	if err := sub.Listen(subAddr); err != nil {
		pub.Close()
		sub.Close()
		return nil, fmt.Errorf("zmq sub listen %s: %w", subAddr, err)
	}
	// Subscribe to all messages (empty topic = all).
	if err := sub.SetOption(zmq4.OptionSubscribe, ""); err != nil {
		pub.Close()
		sub.Close()
		return nil, fmt.Errorf("zmq sub set option: %w", err)
	}

	slog.Info("zmq publisher ready", "pub", pubAddr, "sub", subAddr)
	return &Publisher{
		pubSock:     pub,
		subSock:     sub,
		accountMode: accountMode,
		accountID:   accountID,
	}, nil
}

// PublishPrice publishes a price tick on the "prices" topic.
func (p *Publisher) PublishPrice(epic string, payload interface{}) error {
	return p.publish(TopicPrices, epic, payload)
}

// PublishAccount publishes an account update on the "account" topic.
func (p *Publisher) PublishAccount(payload interface{}) error {
	return p.publish(TopicAccount, "", payload)
}

// PublishTrade publishes a trade event on the "trades" topic.
func (p *Publisher) PublishTrade(payload interface{}) error {
	return p.publish(TopicTrades, "", payload)
}

// ReadOrderCommand blocks until an order command message arrives on the SUB
// socket.  Returns nil when ctx is cancelled.
func (p *Publisher) ReadOrderCommand(ctx context.Context) (*OrderCommand, error) {
	msg, err := p.subSock.Recv()
	if err != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		return nil, fmt.Errorf("zmq sub recv: %w", err)
	}

	// The message may be single-frame or multi-frame.  Use the last frame as
	// the JSON payload (first frame is the topic if multi-frame).
	var raw []byte
	if len(msg.Frames) == 1 {
		raw = msg.Frames[0]
	} else {
		raw = msg.Frames[len(msg.Frames)-1]
	}

	var cmd OrderCommand
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return nil, fmt.Errorf("unmarshal order command: %w", err)
	}
	return &cmd, nil
}

// Close releases ZeroMQ resources.
func (p *Publisher) Close() {
	p.pubSock.Close()
	p.subSock.Close()
}

// ---------------------------------------------------------------------------
// internal
// ---------------------------------------------------------------------------

func (p *Publisher) publish(topic, epic string, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	env := Envelope{
		Type:        topic,
		AccountMode: p.accountMode,
		AccountID:   p.accountID,
		Epic:        epic,
		Ts:          time.Now().UTC().Format(time.RFC3339Nano),
		Payload:     payloadBytes,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	// Send as a two-frame message: topic + JSON body.
	msg := zmq4.NewMsgFromString([]string{topic, string(envBytes)})
	if err := p.pubSock.Send(msg); err != nil {
		return fmt.Errorf("zmq pub send: %w", err)
	}
	return nil
}
