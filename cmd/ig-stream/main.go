// Command ig-stream connects to IG Markets via the Lightstreamer streaming API
// and provides a ZeroMQ-based message bus for prices, account data and trade
// events.  It also accepts market order commands from a ZeroMQ subscriber
// socket and forwards them to the IG REST API.
//
// Configuration is supplied entirely via environment variables; see
// internal/config for details.
//
// Lightstreamer subscription strategy
// ------------------------------------
// The modern PRICE subscription is used (PRICE:{account}:{epic}) rather than
// the deprecated MARKET subscription, as required by the IG API migration
// documented in ig-python/trading-ig#357 (MARKET decommissioned 2026-05-08).
//
// 22:00 UTC nightly guard
// -----------------------
// Order processing is paused between OrderPauseHour:00 UTC and
// OrderPauseHour+OrderResumeMins UTC to avoid placing orders at market close.
// During this window the Lightstreamer session is disconnected and
// reconnected, ensuring the session token remains valid.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/stuartwalker50/ig-data-stream/internal/config"
	"github.com/stuartwalker50/ig-data-stream/internal/igrest"
	"github.com/stuartwalker50/ig-data-stream/internal/lightstreamer"
	"github.com/stuartwalker50/ig-data-stream/internal/publisher"
	"github.com/stuartwalker50/ig-data-stream/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config error", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("fatal error", "err", err)
		os.Exit(1)
	}
	slog.Info("ig-stream stopped cleanly")
}

// ---------------------------------------------------------------------------
// run orchestrates the full lifecycle.
// ---------------------------------------------------------------------------

func run(ctx context.Context, cfg *config.Config) error {
	// 1. Authenticate with IG REST API.
	igClient := igrest.New(cfg.BaseURL(), cfg.APIKey)
	session, err := igClient.CreateSession(cfg.Username, cfg.Password)
	if err != nil {
		return err
	}

	// 2. Bootstrap: fetch and log pre-existing open positions.
	if err := bootstrapPositions(igClient); err != nil {
		slog.Warn("position bootstrap error (continuing)", "err", err)
	}

	// 3. Open SQLite price store.
	priceStore, err := store.Open(cfg.SQLiteDir)
	if err != nil {
		return err
	}
	defer priceStore.Close()

	// 4. Create ZeroMQ publisher/subscriber.
	pub, err := publisher.New(
		ctx,
		cfg.ZMQPubAddr,
		cfg.ZMQSubAddr,
		strings.ToLower(string(cfg.AccType)),
		cfg.AccNumber,
	)
	if err != nil {
		return err
	}
	defer pub.Close()

	// 5. Connect to Lightstreamer and subscribe.
	lsClient, err := connectAndSubscribe(cfg, session, pub, priceStore)
	if err != nil {
		return err
	}

	// 6. Shared, goroutine-safe order-paused flag.
	var ordersPaused atomic.Bool

	// 7. Start order command reader in background.
	go orderCommandLoop(ctx, igClient, pub, &ordersPaused)

	// 8. Channels for the nightly guard to signal pause/resume to the main loop.
	//    Buffered with size 1 so the guard goroutine never blocks.
	pauseCh := make(chan struct{}, 1)
	resumeCh := make(chan struct{}, 1)

	go nightlyGuard(ctx, cfg, pauseCh, resumeCh)

	// 9. Main event loop – handles guard signals and stream termination from
	//    a single goroutine to avoid race conditions on lsClient/session.
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutdown signal received")
			lsClient.Disconnect()
			return nil

		case <-lsClient.Done():
			slog.Warn("lightstreamer stream ended unexpectedly; attempting reconnection")
			lsClient.Disconnect()
			
			// Implement exponential backoff retry logic
			delay := time.Duration(cfg.InitialRetryDelay) * time.Second
			maxDelay := time.Duration(cfg.MaxRetryDelay) * time.Second
			
			var reconnected bool
			for attempt := 1; attempt <= cfg.MaxReconnectAttempts; attempt++ {
				if attempt > 1 {
					slog.Info("stream reconnection retry",
						"attempt", attempt,
						"max_attempts", cfg.MaxReconnectAttempts,
						"delay", delay,
					)
				}
				
				// Wait before retry (exponential backoff)
				time.Sleep(delay)
				
				// Re-authenticate with IG REST API
				newSession, err := igClient.CreateSession(cfg.Username, cfg.Password)
				if err != nil {
					slog.Warn("stream reconnect re-auth failed",
						"attempt", attempt,
						"max_attempts", cfg.MaxReconnectAttempts,
						"err", err,
					)
					delay = delay * 2
					if delay > maxDelay {
						delay = maxDelay
					}
					continue
				}
				
				// Attempt to reconnect stream
				newLS, err := connectAndSubscribe(cfg, newSession, pub, priceStore)
				if err != nil {
					slog.Warn("stream reconnect failed",
						"attempt", attempt,
						"max_attempts", cfg.MaxReconnectAttempts,
						"err", err,
					)
					delay = delay * 2
					if delay > maxDelay {
						delay = maxDelay
					}
					continue
				}
				
				// Success!
				session = newSession
				lsClient = newLS
				reconnected = true
				slog.Info("stream reconnection successful", "attempt", attempt)
				break
			}
			
			if !reconnected {
				return fmt.Errorf("stream reconnection failed after %d attempts", cfg.MaxReconnectAttempts)
			}

		case <-pauseCh:
			slog.Info("nightly guard: pausing orders and reconnecting stream")
			ordersPaused.Store(true)
			lsClient.Disconnect()
			time.Sleep(5 * time.Second)
			newSession, err := igClient.CreateSession(cfg.Username, cfg.Password)
			if err != nil {
				slog.Error("nightly guard: re-auth failed", "err", err)
				continue
			}
			session = newSession
			newLS, err := connectAndSubscribe(cfg, session, pub, priceStore)
			if err != nil {
				slog.Error("nightly guard: stream reconnect failed", "err", err)
				continue
			}
			lsClient = newLS
			slog.Info("nightly guard: stream reconnected")

		case <-resumeCh:
			ordersPaused.Store(false)
			slog.Info("nightly guard: orders resumed")
		}
	}
}

// ---------------------------------------------------------------------------
// connectAndSubscribe creates a fresh Lightstreamer client and registers all
// subscriptions.
// ---------------------------------------------------------------------------

func connectAndSubscribe(
	cfg *config.Config,
	session *igrest.Session,
	pub *publisher.Publisher,
	priceStore *store.Store,
) (*lightstreamer.Client, error) {
	lsPassword := "CST-" + session.CST + "|XST-" + session.XSecurityToken

	lsClient := lightstreamer.NewWithRetry(
		session.LightstreamerEndpoint,
		cfg.AccNumber,
		lsPassword,
		cfg.MaxReconnectAttempts,
		time.Duration(cfg.InitialRetryDelay)*time.Second,
		time.Duration(cfg.MaxRetryDelay)*time.Second,
	)
	if err := lsClient.Connect(); err != nil {
		return nil, err
	}

	// --- PRICE subscription (replaces deprecated MARKET subscription) ---
	// Item format: PRICE:{accountId}:{epic}  (see trading-ig#357)
	priceItems := make([]string, len(cfg.Epics))
	for i, epic := range cfg.Epics {
		priceItems[i] = "PRICE:" + cfg.AccNumber + ":" + epic
	}
	priceFields := []string{
		"TIMESTAMP", "BIDPRICE1", "ASKPRICE1",
		"NET_CHG", "DLG_FLAG", "NET_CHG_", "HIGH", "LOW",
	}
	if _, err := lsClient.Subscribe(
		lightstreamer.ModeMERGE,
		priceItems,
		priceFields,
		"Pricing",
		makePriceListener(cfg, pub, priceStore),
	); err != nil {
		lsClient.Disconnect()
		return nil, err
	}

	// --- ACCOUNT subscription ---
	accountItems := []string{"ACCOUNT:" + cfg.AccNumber}
	accountFields := []string{"FUNDS", "MARGIN", "AVAILABLE_TO_DEAL", "PNL", "EQUITY", "EQUITY_USED"}
	if _, err := lsClient.Subscribe(
		lightstreamer.ModeMERGE,
		accountItems,
		accountFields,
		"",
		makeAccountListener(pub),
	); err != nil {
		lsClient.Disconnect()
		return nil, err
	}

	// --- TRADE subscription ---
	tradeItems := []string{"TRADE:" + cfg.AccNumber}
	tradeFields := []string{"CONFIRMS", "OPU", "WOU"}
	if _, err := lsClient.Subscribe(
		lightstreamer.ModeDISTINCT,
		tradeItems,
		tradeFields,
		"",
		makeTradeListener(pub),
	); err != nil {
		lsClient.Disconnect()
		return nil, err
	}

	return lsClient, nil
}

// ---------------------------------------------------------------------------
// subscription listeners
// ---------------------------------------------------------------------------

// makePriceListener returns a Lightstreamer UpdateListener that normalises
// PRICE subscription updates, persists them to SQLite and publishes them over
// ZeroMQ.
func makePriceListener(cfg *config.Config, pub *publisher.Publisher, priceStore *store.Store) lightstreamer.UpdateListener {
	return func(_ int, pos int, values map[string]string) {
		// Derive the epic from the item position (1-based).
		epicIdx := pos - 1
		if epicIdx < 0 || epicIdx >= len(cfg.Epics) {
			return
		}
		epic := cfg.Epics[epicIdx]

		bid := parseFloat(values["BIDPRICE1"])
		ask := parseFloat(values["ASKPRICE1"])
		tsMs := parseInt64(values["TIMESTAMP"])
		high := parseFloat(values["HIGH"])
		low := parseFloat(values["LOW"])

		if bid == 0 && ask == 0 {
			return // skip empty / unchanged update
		}

		payload := map[string]interface{}{
			"bid":         bid,
			"ask":         ask,
			"net_chg":     parseFloat(values["NET_CHG"]),
			"net_chg_pct": parseFloat(values["NET_CHG_"]),
			"high":        high,
			"low":         low,
			"state":       strings.TrimSpace(values["DLG_FLAG"]),
		}

		now := time.Now().UTC()

		if err := priceStore.WriteTick(store.Tick{
			ReceivedAt:    now,
			Epic:          epic,
			Bid:           bid,
			Ask:           ask,
			IGTimestampMs: tsMs,
			High:          high,
			Low:           low,
		}); err != nil {
			slog.Warn("sqlite write error", "epic", epic, "err", err)
		}

		if err := pub.PublishPrice(epic, payload); err != nil {
			slog.Warn("zmq publish price error", "epic", epic, "err", err)
		}

		slog.Info("price",
			"epic", epic,
			"bid", bid,
			"ask", ask,
			"state", values["DLG_FLAG"],
		)
	}
}

// makeAccountListener returns a listener for ACCOUNT subscription updates.
func makeAccountListener(pub *publisher.Publisher) lightstreamer.UpdateListener {
	return func(_ int, _ int, values map[string]string) {
		payload := map[string]interface{}{
			"funds":          parseFloat(values["FUNDS"]),
			"margin":         parseFloat(values["MARGIN"]),
			"available":      parseFloat(values["AVAILABLE_TO_DEAL"]),
			"pnl":            parseFloat(values["PNL"]),
			"equity":         parseFloat(values["EQUITY"]),
			"equity_used_pct": parseFloat(values["EQUITY_USED"]),
		}
		if err := pub.PublishAccount(payload); err != nil {
			slog.Warn("zmq publish account error", "err", err)
		}
		slog.Info("account",
			"available", values["AVAILABLE_TO_DEAL"],
			"pnl", values["PNL"],
		)
	}
}

// makeTradeListener returns a listener for TRADE subscription updates.
// CONFIRMS, OPU and WOU are themselves JSON strings as published by IG.
func makeTradeListener(pub *publisher.Publisher) lightstreamer.UpdateListener {
	return func(_ int, _ int, values map[string]string) {
		payload := map[string]json.RawMessage{}
		for _, field := range []string{"CONFIRMS", "OPU", "WOU"} {
			if v := values[field]; v != "" {
				payload[strings.ToLower(field)] = json.RawMessage(v)
			}
		}
		if len(payload) == 0 {
			return
		}
		if err := pub.PublishTrade(payload); err != nil {
			slog.Warn("zmq publish trade error", "err", err)
		}
		slog.Info("trade update received",
			"confirms", values["CONFIRMS"] != "",
			"opu", values["OPU"] != "",
			"wou", values["WOU"] != "",
		)
	}
}

// ---------------------------------------------------------------------------
// order command loop
// ---------------------------------------------------------------------------

// orderCommandLoop reads order commands from the ZeroMQ SUB socket and
// forwards them to IG.  ordersPaused is accessed via atomic load so it is
// safe to call from a separate goroutine.
func orderCommandLoop(ctx context.Context, igClient *igrest.Client, pub *publisher.Publisher, ordersPaused *atomic.Bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cmd, err := pub.ReadOrderCommand(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("order command read error", "err", err)
			continue
		}

		if ordersPaused.Load() {
			slog.Warn("order rejected: processing paused (nightly guard)",
				"epic", cmd.Epic, "direction", cmd.Direction)
			continue
		}

		slog.Info("executing order",
			"epic", cmd.Epic,
			"direction", cmd.Direction,
			"size", cmd.Size,
		)

		ref, err := igClient.PlaceMarketOrder(igrest.OrderCommand{
			Epic:           cmd.Epic,
			Direction:      cmd.Direction,
			Size:           cmd.Size,
			OrderType:      cmd.OrderType,
			CurrencyCode:   cmd.CurrencyCode,
			Expiry:         cmd.Expiry,
			ForceOpen:      cmd.ForceOpen,
			GuaranteedStop: cmd.GuaranteedStop,
		})
		if err != nil {
			slog.Error("order placement failed", "epic", cmd.Epic, "err", err)
			continue
		}
		slog.Info("order placed", "deal_reference", ref.DealReference)
	}
}

// ---------------------------------------------------------------------------
// nightly guard
// ---------------------------------------------------------------------------

// nightlyGuard sends to pauseCh at OrderPauseHour:00 UTC each day and to
// resumeCh OrderResumeMins minutes later.  Channels are buffered so sends
// never block if the main loop is momentarily busy.
func nightlyGuard(ctx context.Context, cfg *config.Config, pauseCh, resumeCh chan<- struct{}) {
	for {
		now := time.Now().UTC()
		next := nextOccurrence(now, cfg.OrderPauseHour, 0)
		slog.Info("nightly guard scheduled", "next_pause", next.Format(time.RFC3339))

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}

		select {
		case pauseCh <- struct{}{}:
		case <-ctx.Done():
			return
		}

		// Schedule resume.
		resumeAt := next.Add(time.Duration(cfg.OrderResumeMins) * time.Minute)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(resumeAt)):
		}

		select {
		case resumeCh <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}
}

// nextOccurrence returns the next UTC time when hour:minute occurs (today if
// still in the future, tomorrow otherwise).
func nextOccurrence(now time.Time, hour, minute int) time.Time {
	candidate := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
	if !candidate.After(now) {
		candidate = candidate.Add(24 * time.Hour)
	}
	return candidate
}

// ---------------------------------------------------------------------------
// position bootstrap
// ---------------------------------------------------------------------------

// bootstrapPositions fetches and logs pre-existing open positions so that
// downstream consumers can initialise their state.
func bootstrapPositions(igClient *igrest.Client) error {
	positions, err := igClient.GetPositions()
	if err != nil {
		return err
	}
	if len(positions) == 0 {
		slog.Info("no pre-existing open positions")
		return nil
	}
	for _, p := range positions {
		slog.Info("open position",
			"deal_id", p.DealID,
			"epic", p.Epic,
			"direction", p.Direction,
			"size", p.Size,
			"level", p.Level,
		)
	}
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
