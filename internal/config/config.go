// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// AccType represents whether we are using a live or demo IG account.
type AccType string

const (
	AccTypeLive AccType = "LIVE"
	AccTypeDemo AccType = "DEMO"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	// IG credentials
	Username string
	Password string
	APIKey   string
	AccType  AccType
	AccNumber string

	// Epics to subscribe to (PRICE subscription)
	Epics []string

	// ZeroMQ addresses
	ZMQPubAddr string // outbound price/account/trade messages
	ZMQSubAddr string // inbound order command messages

	// SQLite storage directory
	SQLiteDir string

	// Hour (UTC) at which order processing pauses for nightly maintenance (default 22)
	OrderPauseHour int
	// Duration in minutes after OrderPauseHour at which orders resume (default 30)
	OrderResumeMins int

	// Reconnection retry parameters
	MaxReconnectAttempts int // Maximum attempts to reconnect after stream failure (default 10)
	InitialRetryDelay    int // Initial retry delay in seconds (default 2)
	MaxRetryDelay        int // Maximum retry delay in seconds (default 300, i.e., 5 minutes)
}

// Load reads configuration from environment variables, returning an error if
// any required variable is missing or invalid.
func Load() (*Config, error) {
	required := func(key string) (string, error) {
		v := os.Getenv(key)
		if v == "" {
			return "", fmt.Errorf("required environment variable %s is not set", key)
		}
		return v, nil
	}

	optional := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}

	username, err := required("IG_USERNAME")
	if err != nil {
		return nil, err
	}
	password, err := required("IG_PASSWORD")
	if err != nil {
		return nil, err
	}
	apiKey, err := required("IG_API_KEY")
	if err != nil {
		return nil, err
	}
	accNumber, err := required("IG_ACC_NUMBER")
	if err != nil {
		return nil, err
	}
	epicsRaw, err := required("IG_EPICS")
	if err != nil {
		return nil, err
	}

	accTypeStr := strings.ToUpper(optional("IG_ACC_TYPE", "DEMO"))
	var accType AccType
	switch accTypeStr {
	case "LIVE":
		accType = AccTypeLive
	case "DEMO":
		accType = AccTypeDemo
	default:
		return nil, fmt.Errorf("IG_ACC_TYPE must be LIVE or DEMO, got %q", accTypeStr)
	}

	epics := []string{}
	for _, e := range strings.Split(epicsRaw, ",") {
		e = strings.TrimSpace(e)
		if e != "" {
			epics = append(epics, e)
		}
	}
	if len(epics) == 0 {
		return nil, fmt.Errorf("IG_EPICS must contain at least one epic")
	}

	pauseHour := 22
	if v := os.Getenv("ORDER_PAUSE_HOUR"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 23 {
			return nil, fmt.Errorf("ORDER_PAUSE_HOUR must be an integer 0-23, got %q", v)
		}
		pauseHour = n
	}

	resumeMins := 30
	if v := os.Getenv("ORDER_RESUME_MINS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 1440 {
			return nil, fmt.Errorf("ORDER_RESUME_MINS must be an integer 0-1440, got %q", v)
		}
		resumeMins = n
	}

	maxReconnectAttempts := 10
	if v := os.Getenv("MAX_RECONNECT_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("MAX_RECONNECT_ATTEMPTS must be a positive integer, got %q", v)
		}
		maxReconnectAttempts = n
	}

	initialRetryDelay := 2
	if v := os.Getenv("INITIAL_RETRY_DELAY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("INITIAL_RETRY_DELAY must be a positive integer, got %q", v)
		}
		initialRetryDelay = n
	}

	maxRetryDelay := 300
	if v := os.Getenv("MAX_RETRY_DELAY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("MAX_RETRY_DELAY must be a positive integer, got %q", v)
		}
		maxRetryDelay = n
	}

	return &Config{
		Username:             username,
		Password:             password,
		APIKey:               apiKey,
		AccType:              accType,
		AccNumber:            accNumber,
		Epics:                epics,
		ZMQPubAddr:           optional("ZMQ_PUB_ADDR", "tcp://127.0.0.1:5555"),
		ZMQSubAddr:           optional("ZMQ_SUB_ADDR", "tcp://127.0.0.1:5556"),
		SQLiteDir:            optional("SQLITE_DIR", "."),
		OrderPauseHour:       pauseHour,
		OrderResumeMins:      resumeMins,
		MaxReconnectAttempts: maxReconnectAttempts,
		InitialRetryDelay:    initialRetryDelay,
		MaxRetryDelay:        maxRetryDelay,
	}, nil
}

// BaseURL returns the IG REST API base URL for the configured account type.
func (c *Config) BaseURL() string {
	if c.AccType == AccTypeLive {
		return "https://api.ig.com/gateway/deal"
	}
	return "https://demo-api.ig.com/gateway/deal"
}
