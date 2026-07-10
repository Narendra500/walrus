# Walrus

Walrus is a highly concurrent, in-memory key-value data store built from the ground up in Rust. It is fully compatible with the Redis Serialization Protocol (RESP), meaning it functions as a drop-in backend for standard Redis clients and utilities like `redis-cli` and `redis-benchmark`.

Designed for maximum throughput and predictable latency, Walrus leverages the Tokio asynchronous runtime and a highly optimized, custom byte-parsing engine to handle millions of requests per second.

## Key Features

* **RESP Compatibility:** Implements a custom, highly optimized network parser to safely decode standard RESP frames (Strings, Arrays, Integers, Bulk Strings, Errors) directly from TCP buffers, perfectly compacting memory upon allocation to prevent long-term fragmentation.
* **Massive Concurrency:** Built on `tokio` for non-blocking I/O, capable of multiplexing thousands of concurrent client connections across multi-core systems.
* **Fine-Grained Sharding:** Utilizes DashMap for the core key-value storage, employing lock-striping to partition the database into independent shards. This completely eliminates global lock contention and drastically reduces futex wait times during high-frequency parallel reads and writes.
* **Advanced Cross-Connection Synchronization:** Supports true blocking commands like `BLPOP` using `tokio::sync::Notify`, allowing isolated TCP connections to signal and wake each other instantly without thread blocking or CPU polling.
* **Precise Expiration (TTL):** Features an event-driven, background eviction system using a synchronous `Mutex<BTreeSet>` to track and purge expired keys with microsecond precision, avoiding the overhead of O(N) memory scanning.

## Supported Commands

* **Keys & Strings:** `GET`, `SET` (with `EX` and `PX` expiration support), `Type`
* **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `BLPOP`
* **Connection & Utility:** `PING`

## Architecture Highlights

Walrus was engineered to minimize system calls and maximize network card utilization:

* **Smart Batching & Pipelining:** Uses dynamic write buffer, allowing the server to parse and process heavily pipelined requests (e.g., 32+ commands per TCP frame) while executing only a single OS `write` syscall per event loop iteration.
* **TCP Optimization:** Explicitly disables Nagle's algorithm (`TCP_NODELAY`) to prevent artificial kernel buffering, ensuring microsecond latency on small command payloads.
* **Defensive Parsing:** The RESP parser handles TCP fragmentation natively, verifying payload boundaries before advancing buffer cursors to prevent out-of-bounds panics on incomplete network reads.

## Performance

Walrus has been extensively profiled using `perf` and flamegraphs to eliminate kernel-level blocking. Walrus achieves exceptional throughput on standard hardware:

**valkey-benchmark -p 6380 -n 1000000 -t get,set,lpop,lrange -c 500 -q --threads 12**
* `SET`: 568504.88 requests per second, p50=0.495 msec
* `GET`: 569152.00 requests per second, p50=0.495 msec
* `LPOP`: 567859.19 requests per second, p50=0.463 msec
* `LPUSH`: 568828.19 requests per second, p50=0.487 msec
* `LRANGE_100` (first 100 elements): 442477.88 requests per second, p50=0.607 msec
* `LRANGE_300` (first 300 elements): 264620.28 requests per second, p50=1.015 msec
* `LRANGE_500` (first 500 elements): 180799.12 requests per second, p50=1.447 msec
* `LRANGE_600` (first 600 elements): 158679.78 requests per second, p50=1.671 msec

**valkey-benchmark -p 6380 -n 5000000 -t get,set,lpop,lrange -c 500 -q -P 32 --threads 12**
* `SET`: 4960317.50 requests per second, p50=2.687 msec
* `GET`: 6605019.50 requests per second, p50=1.551 msec
* `LPOP`: 2849002.75 requests per second, p50=5.039 msec
* `LPUSH`: 2849002.75 requests per second, p50=4.343 msec
* `LRANGE_100` (first 100 elements): 1316482.38 requests per second, p50=5.559 msec
* `LRANGE_300` (first 300 elements): 428302.22 requests per second, p50=8.983 msec
* `LRANGE_500` (first 500 elements): 246135.69 requests per second, p50=10.559 msec
* `LRANGE_600` (first 600 elements): 207125.11 requests per second, p50=10.855 msec

**valkey-benchmark -p 6380 -n 1000000 -t get,set,lpop,lrange -c 50 -q --threads 12**
* `SET`: 399520.56 requests per second, p50=0.095 msec
* `GET`: 400000.00 requests per second, p50=0.087 msec
* `LPOP`: 399680.25 requests per second, p50=0.087 msec
* `LPUSH`: 399520.56 requests per second, p50=0.087 msec
* `LRANGE_100` (first 100 elements): 333000.34 requests per second, p50=0.095 msec
* `LRANGE_300` (first 300 elements): 221975.58 requests per second, p50=0.127 msec
* `LRANGE_500` (first 500 elements): 166444.75 requests per second, p50=0.159 msec
* `LRANGE_600` (first 600 elements): 147950.89 requests per second, p50=0.175 msec

**valkey-benchmark -p 6380 -n 10000000 -t get,set,lpop,lrange -c 1000 -P 32 -q --threads 12**
* `SET`: 5662514.00 requests per second, p50=4.839 msec
* `GET`: 7930214.00 requests per second, p50=2.999 msec
* `LPOP`: 7930214.00 requests per second, p50=3.103 msec
* `LPUSH`: 2658160.50 requests per second, p50=8.951 msec
* `LRANGE_100` (first 100 elements): 1355197.12 requests per second, p50=9.951 msec
* `LRANGE_300` (first 300 elements): 423423.78 requests per second, p50=15.855 msec
* `LRANGE_500` (first 500 elements): 249028.80 requests per second, p50=19.295 msec
* `LRANGE_600` (first 600 elements): 209762.34 requests per second, p50=19.663 msec

*(Note: Benchmarks run on Intel i5 12450hx CPU)*

## Getting Started
### Installation

Clone the repository and build the project in release mode for optimal performance.

```bash
git clone https://github.com/Narendra500/walrus.git
cd walrus
cargo build --release

```

### Running the Server

Start the Walrus server. By default, it binds to `127.0.0.1:6380`.

```bash
cargo run --release --bin server

```

To run on a different port
```bash
cargo run --release --bin server -- -p <PORT> 

```

### Connecting

You can connect to Walrus using the standard Redis CLI or Valkey CLI.

```bash
redis-cli -p 6380
valkey-cli -p 6380

```

```text
127.0.0.1:6380> SET database "Walrus"
OK
127.0.0.1:6380> GET database
"Walrus"

```

## Testing

Run the standard test suite:

```bash
cargo test

```

To run integration benchmarks, use `redis-benchmark` or `valkey-benchmark`:

```bash
redis-benchmark -p 6380 -c 50 -n 100000 -q
valkey-benchmark -p 6380 -c 50 -n 100000 -q

```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
