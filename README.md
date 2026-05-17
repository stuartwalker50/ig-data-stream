# ig-data-stream

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
