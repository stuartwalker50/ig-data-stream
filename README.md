# ig-data-stream

A Go service that connects to IG Markets via the Lightstreamer Streaming API,
publishes normalised market data over ZeroMQ, and executes order commands
received from strategy services.

## Quick start

```bash
# Copy and edit the environment file
cp .env.example .env   # see Environment variables section below

# Build
go build -o ig-stream ./cmd/ig-stream

# Run
source .env && ./ig-stream
```

> `ig-stream` reads exported environment variables. The `.env.example` file
> uses `export` so `source .env` makes values visible to the process.

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `IG_USERNAME` | ✓ | – | IG account username |
| `IG_PASSWORD` | ✓ | – | IG account password |
| `IG_API_KEY` | ✓ | – | IG API key |
| `IG_ACC_NUMBER` | ✓ | – | IG account number (e.g. `ZXXXXX`) |
| `IG_ACC_TYPE` | | `DEMO` | `LIVE` or `DEMO` |
| `IG_EPICS` | ✓ | – | Comma-separated list of epics to subscribe |
| `ZMQ_PUB_ADDR` | | `tcp://127.0.0.1:5555` | ZeroMQ PUB socket bind address |
| `ZMQ_SUB_ADDR` | | `tcp://127.0.0.1:5556` | ZeroMQ SUB socket bind address (order commands) |
| `SQLITE_DIR` | | `.` | Directory in which to create price tick databases |
| `ORDER_PAUSE_HOUR` | | `22` | UTC hour at which order processing is paused (0-23) |
| `ORDER_RESUME_MINS` | | `30` | Minutes after pause hour before orders resume |
| `MAX_RECONNECT_ATTEMPTS` | | `10` | Maximum reconnection attempts after stream failure |
| `INITIAL_RETRY_DELAY` | | `2` | Initial retry delay in seconds |
| `MAX_RETRY_DELAY` | | `300` | Maximum retry delay in seconds (5 minutes) |

## Architecture overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         ig-stream (Go)                          │
│                                                                  │
│  IG REST API  ──► igrest.Client ──► session tokens             │
│                                       │                          │
│  Lightstreamer  ──► ls.Client ──► PRICE/ACCOUNT/TRADE subs     │
│    (HTTP text                         │                          │
│     protocol)                    ┌────┴──────────────────┐      │
│                                  │  publisher (ZeroMQ)   │      │
│                                  │  PUB: prices/account/ │◄──── SQLite store │
│                                  │       trades          │      │
│                                  │  SUB: order commands  │      │
│                                  └───────────────────────┘      │
│                                                                  │
│  22:00 UTC guard: pause orders, reconnect stream session        │
└─────────────────────────────────────────────────────────────────┘
```

## ZeroMQ message format

Every published message is a two-frame ZeroMQ message: `[topic, JSON]`.

**Topics**: `prices`, `account`, `trades`

**Envelope** (JSON):
```json
{
  "type": "prices",
  "account_mode": "demo",
  "account_id": "ZXXXXX",
  "epic": "CS.D.EURUSD.MINI.IP",
  "ts": "2026-05-17T18:00:00.000000000Z",
  "payload": { ... }
}
```

**Price payload**:
```json
{ "bid": 1.085, "ask": 1.0852, "net_chg": 0.0002,
  "net_chg_pct": 0.02, "high": 1.09, "low": 1.075, "state": "tradeable" }
```

**Order command** (sent to the SUB socket by strategy services):
```json
{ "epic": "CS.D.EURUSD.MINI.IP", "direction": "BUY", "size": 1.0,
  "order_type": "MARKET", "currency_code": "USD",
  "expiry": "-", "force_open": true, "guaranteed_stop": false }
```

## Lightstreamer subscriptions

Uses the **new PRICE subscription** (`PRICE:{account}:{epic}` / adapter `Pricing`)
which replaced the deprecated `MARKET:` subscription decommissioned 2026-05-08
([trading-ig#357](https://github.com/ig-python/trading-ig/issues/357)).

| Subscription | Mode | Items | Fields |
|---|---|---|---|
| Price | MERGE | `PRICE:{acc}:{epic}` | TIMESTAMP, BIDPRICE1, ASKPRICE1, NET_CHG, DLG_FLAG, NET_CHG_, HIGH, LOW |
| Account | MERGE | `ACCOUNT:{acc}` | FUNDS, MARGIN, AVAILABLE_TO_DEAL, PNL, EQUITY, EQUITY_USED |
| Trade | DISTINCT | `TRADE:{acc}` | CONFIRMS, OPU, WOU |

## SQLite price store

A new database file `prices_YYYYMMDD_HHMMSS.db` is created on each run.
Each file contains a `prices` table suitable for backtesting:

```sql
CREATE TABLE prices (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at  TEXT    NOT NULL,   -- UTC RFC3339 timestamp
    epic         TEXT    NOT NULL,
    ig_ts_ms     INTEGER NOT NULL,   -- IG TIMESTAMP (ms since epoch)
    bid          REAL    NOT NULL,
    ask          REAL    NOT NULL,
    high         REAL,
    low          REAL
);
```

## 22:00 UTC nightly guard

Order execution is paused at 22:00 UTC and the Lightstreamer session is
disconnected and reconnected to keep session tokens valid.  Orders resume at
22:30 UTC.  Orders received during the pause window are rejected with a log
warning.

## Reconnection and error handling

The service implements robust reconnection logic with exponential backoff to handle:

### Unexpected stream disconnections
When the Lightstreamer stream ends unexpectedly (network issues, server-side disconnects), the service:
1. Attempts to re-authenticate with IG REST API
2. Reconnects to Lightstreamer with fresh subscriptions
3. Uses exponential backoff: delays double on each retry (2s → 4s → 8s → 16s...) up to `MAX_RETRY_DELAY`
4. Only exits after `MAX_RECONNECT_ATTEMPTS` exhausted

### LOOP rebind failures
When Lightstreamer sends a LOOP message (normal server-side session rebind), the service:
1. Attempts to rebind the existing session
2. Uses exponential backoff on failures (2s → 4s → 8s → 16s...) up to `MAX_RETRY_DELAY`
3. Only closes connection after `MAX_RECONNECT_ATTEMPTS` exhausted

All retry attempts are logged with attempt number and delay for troubleshooting.

---

## Architecture decision brief

Analysis and implementation guidance for an IG Markets streaming/order pipeline.

## Functional scope (from issue)

- Connect to IG Markets Streaming API (live/demo).
- Subscribe to:
  - Prices (hardcoded epic list).
  - Account metrics (available cash, PnL).
  - Trade events (confirms, open order updates, working order updates).
- Bootstrap pre-existing open positions at startup.
- Accept incoming order commands and place market orders via IG REST API.
- Pause/guard order processing around `22:00 UTC` and reconnect safely while keeping session validity.
- Persist live prices to SQLite with timestamped DB filename for offline backup/backtesting.

## Implementation options

### 1) Python-centric service (fastest delivery)

**Languages/libraries**
- Python 3.11+
- `trading-ig` (IG REST + streaming helpers)
- `pydantic` for normalized message schemas
- `pyzmq` (if keeping ZeroMQ)
- `sqlite3` (stdlib) or `aiosqlite`
- `APScheduler` or `asyncio` scheduling for 22:00 guard window

**Pros**
- Fast to build and iterate.
- Strong ecosystem for trading/data pipelines.
- Good fit for SQLite persistence and backtest tooling.

**Cons**
- Lower raw throughput than Go/Java for very high message rates.

---

### 2) Go service (throughput + operational simplicity)

**Languages/libraries**
- Go 1.22+
- IG integration via REST/Lightstreamer client (community SDK or direct integration)
- `nats.go` / `segmentio/kafka-go` / `go-zeromq` (transport dependent)
- `modernc.org/sqlite` or CGO sqlite driver

**Pros**
- Strong concurrency model, static binaries, efficient runtime.
- Great for resilient always-on daemons.

**Cons**
- IG-specific client ecosystem is smaller than Python’s.

---

### 3) JVM/.NET service (enterprise integrations)

**Languages/libraries**
- Java/Kotlin + official Lightstreamer Java client, or C# with Lightstreamer .NET client
- Kafka/NATS/RabbitMQ clients
- JDBC/EF for persistence

**Pros**
- Mature streaming clients and robust long-running service tooling.
- Strong typing, observability, and enterprise patterns.

**Cons**
- Heavier setup than Python/Go.

## Is there a better way than ZeroMQ for multi-language modularity?

Short answer: **yes, for most production use-cases, a brokered event bus is better**.

### ZeroMQ strengths
- Very fast, lightweight socket patterns.
- Great for local low-latency components.

### ZeroMQ limitations for cross-language modularity
- No central broker (harder discovery, replay, durability, backpressure, operations).
- Message retention/recovery is DIY.
- Debug/ops story grows complex as components increase.

### Better alternatives

1. **NATS (+ JetStream)** — recommended default  
   - Excellent multi-language support.
   - Simple operations vs Kafka.
   - Request/reply + pub/sub + durable streams in one system.
   - Good fit for order command channel + market event fan-out.

2. **Kafka/Redpanda** — best for long retention + analytics scale  
   - Strong replay/history for backtesting and audit.
   - Heavier operational footprint.

3. **RabbitMQ** — mature queue semantics  
   - Strong routing/work queues.
   - Less ideal than NATS/Kafka for high-throughput market tick streaming.

## Best-approach candidates

### Candidate A (recommended): Python + NATS JetStream + SQLite

- **Ingestion service (Python)**:
  - Connect IG stream.
  - Normalize events.
  - Publish to `prices.*`, `account.*`, `trades.*`.
- **Order service**:
  - Subscribe `orders.market`.
  - Enforce 22:00 UTC order guard.
  - Execute via IG REST.
  - Publish confirms/failures.
- **Storage writer**:
  - Subscribe price subjects.
  - Batch-write to timestamped SQLite DB file.

Why this is best here:
- Minimal complexity with strong modularity across languages.
- Easy to add future consumers in Go/Node/Java/C#.
- Durable stream option without building custom replay/recovery logic.

### Candidate B: Keep ZeroMQ now, add compatibility boundary

- Keep existing PUB/SUB sockets.
- Introduce strict schema contracts (`JSON`/`Protobuf`) and a transport abstraction layer.
- Later swap transport to NATS/Kafka without changing business logic.

Good transitional approach if immediate re-platform risk must be minimized.

## Suggested message contract

- Envelope fields: `type`, `source`, `account_mode`, `epic`, `ts`, `correlation_id`, `payload`.
- Use UTC ISO-8601 timestamps.
- Prefer Protobuf/Avro for long-term multi-language compatibility (JSON acceptable initially).

## Practical recommendation

If starting fresh or able to refactor now, choose:

- **Core language**: Python (delivery speed + IG ecosystem)
- **Transport**: **NATS JetStream** (better modularity than ZeroMQ)
- **Storage**: SQLite rolling files (timestamped)
- **Schema**: Versioned envelope + typed payloads

If constrained by existing ZeroMQ components, adopt Candidate B first, then migrate transport incrementally.
