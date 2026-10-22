# CoreBook 🦀

> Ultra-Low-Latency Limit Order Book & Continuous Double Auction Matching Engine built in Rust.

`CoreBook` is a high-performance quantitative system engineered in Rust to process real-time Layer 2 market feeds, manage an in-memory Limit Order Book (LOB), enforce strict crossed-book invariants, and execute price-time priority matching with sub-microsecond latencies.

![Rust](https://img.shields.io/badge/Language-Rust-orange.svg)
![Tokio](https://img.shields.io/badge/Async-Tokio-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

---

## Architecture Overview

1. **Asynchronous Feed Ingestion:** Connects via asynchronous WebSockets (`tokio-tungstenite`) to Coinbase Advanced Trade live tickers (`BTC-USD`).
2. **In-Memory Order Book:** Maintains price-time priority queues for Bids and Asks using memory-efficient Rust vectors.
3. **Continuous Double Auction:** Validates incoming order types (crossing taker vs. resting maker) and handles partial fills while guaranteeing zero crossed-book invariant violations.
4. **Sub-Microsecond Benchmarking:** Uses `std::time::Instant` to capture real-time matching telemetry without Garbage Collection (GC) pauses.
5. **Real-Time Telemetry Stream:** Spawns a local WebSocket server (`ws://127.0.0.1:8080`) to broadcast execution logs and market depth to a clean institutional dashboard.

---

## Tech Stack

* **Language:** Rust
* **Async Runtime:** `tokio`
* **Networking:** `tokio-tungstenite`, `futures-util`
* **Serialization:** `serde_json`
* **Frontend Dashboard:** HTML5, JavaScript, Tailwind CSS 

---

## Quickstart

### Prerequisites
* [Rust & Cargo](https://www.rust-lang.org/tools/install) installed.

### 1. Run the Matching Engine
```bash
cargo run --release