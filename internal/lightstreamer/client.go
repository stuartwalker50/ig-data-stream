// Package lightstreamer provides a client for the Lightstreamer HTTP text
// streaming protocol used by the IG Markets streaming API.
//
// The implementation follows the same protocol used by trading-ig's
// lightstreamer.py reference implementation.  Subscriptions use the modern
// PRICE item names (PRICE:{account}:{epic}) rather than the deprecated
// MARKET names, per ig-python/trading-ig#357.
package lightstreamer

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Subscription mode constants.
const (
	ModeMERGE    = "MERGE"
	ModeDISTINCT = "DISTINCT"
)

// Internal Lightstreamer text-protocol path suffixes.
const (
	pathCreate  = "lightstreamer/create_session.txt"
	pathBind    = "lightstreamer/bind_session.txt"
	pathControl = "lightstreamer/control.txt"
)

// lsCID is the client ID required by the Lightstreamer text protocol.
const lsCID = "mgQkwtwdysogQz2BJ4Ji kOj2Bg"

// contentLength is a large value requested to keep the streaming body open.
const contentLength = "50000000"

// UpdateListener is called for every item update received from Lightstreamer.
// pos is the 1-based position of the item in the subscription's item list.
// values maps field names to their decoded string values (empty string means
// the value is unchanged from the previous update in MERGE mode; use the last
// known value in that case).
type UpdateListener func(subscriptionKey int, pos int, values map[string]string)

// subscription tracks an active subscription registered with the server.
type subscription struct {
	mode       string
	items      []string
	fields     []string
	adapter    string
	prevValues map[int]map[string]string // item pos → last known field values
	listener   UpdateListener
}

// Client manages a Lightstreamer text-protocol streaming session.
type Client struct {
	baseURL    string
	user       string
	password   string
	httpClient *http.Client

	mu              sync.Mutex
	session         map[string]string
	controlURL      string
	subKey          int
	subscriptions   map[int]*subscription
	streamBody      io.ReadCloser
	streamScanner   *bufio.Scanner

	// done is closed exactly once when the session ends permanently.
	// It is never replaced; use doneOnce to guard the single close.
	done     chan struct{}
	doneOnce sync.Once
}

// New creates a new Lightstreamer Client.
func New(baseURL, user, password string) *Client {
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		user:     user,
		password: password,
		httpClient: &http.Client{
			Timeout: 0, // streaming – no overall timeout
		},
		subscriptions: make(map[int]*subscription),
		done:          make(chan struct{}),
	}
}

// Connect establishes a session with the Lightstreamer server and starts the
// background reader goroutine.
func (c *Client) Connect() error {
	params := url.Values{
		"LS_op2":           {"create"},
		"LS_cid":           {lsCID},
		"LS_adapter_set":   {"DEFAULT"},
		"LS_user":          {c.user},
		"LS_password":      {c.password},
		"LS_content_length": {contentLength},
	}
	resp, err := c.httpClient.PostForm(c.baseURL+"/"+pathCreate, params)
	if err != nil {
		return fmt.Errorf("lightstreamer connect: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("lightstreamer connect: HTTP %d", resp.StatusCode)
	}

	if err := c.readSessionHeader(resp.Body); err != nil {
		resp.Body.Close()
		return err
	}

	c.mu.Lock()
	c.streamBody = resp.Body
	c.streamScanner = bufio.NewScanner(resp.Body)
	c.mu.Unlock()

	go c.receive()
	return nil
}

// Subscribe registers a new subscription with the Lightstreamer server and
// attaches a listener. It returns the subscription key that can be used to
// unsubscribe later.
func (c *Client) Subscribe(mode string, items, fields []string, adapter string, listener UpdateListener) (int, error) {
	c.mu.Lock()
	c.subKey++
	key := c.subKey
	c.subscriptions[key] = &subscription{
		mode:       mode,
		items:      items,
		fields:     fields,
		adapter:    adapter,
		prevValues: make(map[int]map[string]string),
		listener:   listener,
	}
	c.mu.Unlock()

	params := url.Values{
		"LS_session":      {c.session["SessionId"]},
		"LS_Table":        {strconv.Itoa(key)},
		"LS_op":           {"add"},
		"LS_data_adapter": {adapter},
		"LS_mode":         {mode},
		"LS_schema":       {strings.Join(fields, " ")},
		"LS_id":           {strings.Join(items, " ")},
	}

	if err := c.control(params); err != nil {
		c.mu.Lock()
		delete(c.subscriptions, key)
		c.mu.Unlock()
		return 0, fmt.Errorf("subscribe (key=%d): %w", key, err)
	}

	slog.Info("lightstreamer subscription added",
		"key", key, "mode", mode, "items", items, "adapter", adapter)
	return key, nil
}

// Unsubscribe removes a previously registered subscription.
func (c *Client) Unsubscribe(key int) error {
	params := url.Values{
		"LS_session": {c.session["SessionId"]},
		"LS_Table":   {strconv.Itoa(key)},
		"LS_op":      {"delete"},
	}
	if err := c.control(params); err != nil {
		return fmt.Errorf("unsubscribe (key=%d): %w", key, err)
	}
	c.mu.Lock()
	delete(c.subscriptions, key)
	c.mu.Unlock()
	return nil
}

// Disconnect closes the streaming connection and signals the background
// goroutine to exit.
func (c *Client) Disconnect() {
	c.closeDone()
	c.mu.Lock()
	if c.streamBody != nil {
		c.streamBody.Close()
	}
	c.mu.Unlock()
}

// Done returns a channel that is closed when the streaming session ends
// permanently (not during a LOOP rebind).
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// ---------------------------------------------------------------------------
// internal helpers
// ---------------------------------------------------------------------------

// closeDone ensures done is closed exactly once, regardless of how many
// goroutines reach terminal states simultaneously.
func (c *Client) closeDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

// readSessionHeader reads the initial OK line and session key:value pairs from
// the streaming response body.
func (c *Client) readSessionHeader(body io.Reader) error {
	scanner := bufio.NewScanner(body)

	// Expect "OK" as the first non-empty line.
	for scanner.Scan() {
		line := scanner.Text()
		if line == "OK" {
			break
		}
		if strings.HasPrefix(line, "ERROR") || strings.HasPrefix(line, "CONERR") {
			return fmt.Errorf("lightstreamer session error: %s", line)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading session header: %w", err)
	}

	c.session = make(map[string]string)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			break
		}
		if parts := strings.SplitN(line, ":", 2); len(parts) == 2 {
			c.session[parts[0]] = parts[1]
		}
	}

	slog.Info("lightstreamer session established",
		"session_id", c.session["SessionId"],
		"control_address", c.session["ControlAddress"],
	)

	// Determine the control URL from ControlAddress if provided.
	c.controlURL = c.baseURL
	if ca, ok := c.session["ControlAddress"]; ok && ca != "" {
		parsed, err := url.Parse(c.baseURL)
		if err == nil {
			parsed.Host = ca
			c.controlURL = parsed.String()
		}
	}

	return nil
}

// control sends a control request to the Lightstreamer server.
func (c *Client) control(params url.Values) error {
	resp, err := c.httpClient.PostForm(c.controlURL+"/"+pathControl, params)
	if err != nil {
		return fmt.Errorf("control request: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	result := strings.TrimSpace(string(raw))
	if result != "OK" {
		return fmt.Errorf("control response: %s", result)
	}
	return nil
}

// receive is the background goroutine that reads streaming updates.
func (c *Client) receive() {
	c.mu.Lock()
	scanner := c.streamScanner
	c.mu.Unlock()

	for scanner.Scan() {
		select {
		case <-c.done:
			return
		default:
		}

		line := scanner.Text()
		switch {
		case line == "":
			// ignore blank lines
		case line == "PROBE":
			slog.Debug("lightstreamer PROBE received")
		case strings.HasPrefix(line, "LOOP"):
			slog.Info("lightstreamer LOOP – rebinding session")
			go c.rebind()
			return // exit without closing done; rebind will restart receive
		case strings.HasPrefix(line, "END"):
			slog.Warn("lightstreamer END – session closed by server", "msg", line)
			c.closeDone()
			return
		case strings.HasPrefix(line, "ERROR"), strings.HasPrefix(line, "SYNC ERROR"):
			slog.Error("lightstreamer error", "msg", line)
			c.closeDone()
			return
		case strings.HasPrefix(line, "Preamble"):
			// skip
		default:
			c.handleUpdate(line)
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("lightstreamer stream read error", "err", err)
	}
	c.closeDone()
}

// rebind re-establishes the streaming connection for an existing session after
// a LOOP message.  It closes done only on unrecoverable errors.
func (c *Client) rebind() {
	// Close and clear the old stream body under the mutex.
	c.mu.Lock()
	if c.streamBody != nil {
		c.streamBody.Close()
		c.streamBody = nil
	}
	sessionID := c.session["SessionId"]
	controlURL := c.controlURL
	c.mu.Unlock()

	// Back-off before rebind attempt.
	time.Sleep(2 * time.Second)

	params := url.Values{
		"LS_session":        {sessionID},
		"LS_content_length": {contentLength},
	}

	resp, err := c.httpClient.PostForm(controlURL+"/"+pathBind, params)
	if err != nil {
		slog.Error("lightstreamer rebind failed", "err", err)
		c.closeDone()
		return
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		slog.Error("lightstreamer rebind HTTP error", "status", resp.StatusCode)
		c.closeDone()
		return
	}

	// Re-read session header after rebind.
	if err := c.readSessionHeader(resp.Body); err != nil {
		resp.Body.Close()
		slog.Error("lightstreamer rebind session header error", "err", err)
		c.closeDone()
		return
	}

	c.mu.Lock()
	c.streamBody = resp.Body
	c.streamScanner = bufio.NewScanner(resp.Body)
	c.mu.Unlock()

	go c.receive()
}

// handleUpdate parses a single update line and dispatches it to the matching
// subscription listener.
//
// Line format: "{tableKey},{itemPos}|{field1}|{field2}|..."
func (c *Client) handleUpdate(line string) {
	// Split at the first comma to get tableKey and the rest.
	commaIdx := strings.Index(line, ",")
	if commaIdx < 0 {
		slog.Debug("lightstreamer unrecognised line", "line", line)
		return
	}

	tableKey, err := strconv.Atoi(line[:commaIdx])
	if err != nil {
		slog.Debug("lightstreamer bad table key", "line", line)
		return
	}

	rest := line[commaIdx+1:]
	pipeIdx := strings.Index(rest, "|")
	if pipeIdx < 0 {
		slog.Debug("lightstreamer no pipe separator", "line", line)
		return
	}

	itemPos, err := strconv.Atoi(rest[:pipeIdx])
	if err != nil {
		slog.Debug("lightstreamer bad item pos", "line", line)
		return
	}

	rawFields := strings.Split(rest[pipeIdx+1:], "|")

	c.mu.Lock()
	sub, ok := c.subscriptions[tableKey]
	c.mu.Unlock()
	if !ok {
		slog.Debug("lightstreamer update for unknown subscription", "key", tableKey)
		return
	}

	// Decode field values according to the Lightstreamer text protocol.
	prev := sub.prevValues[itemPos]
	if prev == nil {
		prev = make(map[string]string)
	}

	values := make(map[string]string, len(sub.fields))
	for i, field := range sub.fields {
		var raw string
		if i < len(rawFields) {
			raw = rawFields[i]
		}
		values[field] = decodeField(raw, prev[field])
	}

	// Update MERGE state cache.
	if sub.mode == ModeMERGE {
		prev = make(map[string]string, len(values))
		for k, v := range values {
			prev[k] = v
		}
		c.mu.Lock()
		sub.prevValues[itemPos] = prev
		c.mu.Unlock()
	}

	if sub.listener != nil {
		sub.listener(tableKey, itemPos, values)
	}
}

// decodeField decodes a single field value from the Lightstreamer text
// protocol, using last as the previous value for unchanged-field handling.
func decodeField(raw, last string) string {
	switch raw {
	case "$":
		return ""
	case "#":
		return "" // null represented as empty string
	case "":
		return last // unchanged in MERGE mode
	}
	// Leading $ or # is an escape character.
	if len(raw) > 0 && (raw[0] == '$' || raw[0] == '#') {
		return raw[1:]
	}
	return raw
}
