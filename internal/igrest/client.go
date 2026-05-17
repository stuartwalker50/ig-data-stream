// Package igrest provides a minimal IG Markets REST API client for session
// management, position bootstrapping and market order placement.
package igrest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// Session holds the tokens returned by a successful IG session creation.
type Session struct {
	CST                  string
	XSecurityToken       string
	LightstreamerEndpoint string
	AccountID            string
}

// Position represents a single open position returned by GET /positions.
type Position struct {
	DealID    string  `json:"dealId"`
	Epic      string  `json:"epic"`
	Direction string  `json:"direction"` // BUY or SELL
	Size      float64 `json:"size"`
	Level     float64 `json:"level"`
}

// OrderCommand is the payload for placing a market order via POST /orders/otc.
type OrderCommand struct {
	Epic           string  `json:"epic"`
	Direction      string  `json:"direction"`
	Size           float64 `json:"size"`
	OrderType      string  `json:"orderType"`
	CurrencyCode   string  `json:"currencyCode"`
	Expiry         string  `json:"expiry"`
	ForceOpen      bool    `json:"forceOpen"`
	GuaranteedStop bool    `json:"guaranteedStop"`
}

// DealReference is the response returned when a deal is accepted.
type DealReference struct {
	DealReference string `json:"dealReference"`
}

// Client is a minimal IG REST API client.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	session    *Session
}

// New creates a new IG REST Client.
func New(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// CreateSession authenticates with the IG REST API and stores the returned
// tokens.  It must be called before any other method.
func (c *Client) CreateSession(username, password string) (*Session, error) {
	body := map[string]interface{}{
		"identifier":        username,
		"password":          password,
		"encryptedPassword": false,
	}
	resp, err := c.doRequest("POST", "/session", "2", body, nil)
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("create session HTTP %d: %s", resp.StatusCode, raw)
	}

	var result struct {
		LightstreamerEndpoint string `json:"lightstreamerEndpoint"`
		CurrentAccountID      string `json:"currentAccountId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode session: %w", err)
	}

	c.session = &Session{
		CST:                  resp.Header.Get("CST"),
		XSecurityToken:       resp.Header.Get("X-SECURITY-TOKEN"),
		LightstreamerEndpoint: result.LightstreamerEndpoint,
		AccountID:            result.CurrentAccountID,
	}

	slog.Info("IG session created",
		"account", c.session.AccountID,
		"endpoint", c.session.LightstreamerEndpoint,
	)
	return c.session, nil
}

// Session returns the current session tokens (nil if not yet authenticated).
func (c *Client) Session() *Session {
	return c.session
}

// GetPositions returns currently open positions for the authenticated account.
func (c *Client) GetPositions() ([]Position, error) {
	resp, err := c.doRequest("GET", "/positions", "2", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get positions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get positions HTTP %d: %s", resp.StatusCode, raw)
	}

	var result struct {
		Positions []struct {
			Position struct {
				DealID    string  `json:"dealId"`
				Direction string  `json:"direction"`
				Size      float64 `json:"size"`
				Level     float64 `json:"level"`
			} `json:"position"`
			Market struct {
				Epic string `json:"epic"`
			} `json:"market"`
		} `json:"positions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode positions: %w", err)
	}

	positions := make([]Position, 0, len(result.Positions))
	for _, p := range result.Positions {
		positions = append(positions, Position{
			DealID:    p.Position.DealID,
			Epic:      p.Market.Epic,
			Direction: p.Position.Direction,
			Size:      p.Position.Size,
			Level:     p.Position.Level,
		})
	}
	return positions, nil
}

// PlaceMarketOrder submits a market order to the IG OTC order endpoint.
func (c *Client) PlaceMarketOrder(cmd OrderCommand) (*DealReference, error) {
	if cmd.OrderType == "" {
		cmd.OrderType = "MARKET"
	}
	if cmd.Expiry == "" {
		cmd.Expiry = "-"
	}
	cmd.ForceOpen = true

	resp, err := c.doRequest("POST", "/orders/otc", "2", cmd, nil)
	if err != nil {
		return nil, fmt.Errorf("place order: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("place order HTTP %d: %s", resp.StatusCode, raw)
	}

	var ref DealReference
	if err := json.NewDecoder(resp.Body).Decode(&ref); err != nil {
		return nil, fmt.Errorf("decode deal reference: %w", err)
	}
	return &ref, nil
}

// doRequest performs an authenticated HTTP request against the IG REST API.
func (c *Client) doRequest(method, path, version string, body interface{}, extraHeaders map[string]string) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("Accept", "application/json; charset=UTF-8")
	req.Header.Set("X-IG-API-KEY", c.apiKey)
	req.Header.Set("Version", version)

	if c.session != nil {
		req.Header.Set("CST", c.session.CST)
		req.Header.Set("X-SECURITY-TOKEN", c.session.XSecurityToken)
	}

	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	return c.httpClient.Do(req)
}
