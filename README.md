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

* **Keys & Strings:** `GET`, `SET` (with `EX` and 'PX' expiration support), `Type`
* **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `BLPOP`
* **Connection & Utility:** `PING`

## Architecture Highlights

Walrus was engineered to minimize system calls and maximize network card utilization:

* **Smart Batching & Pipelining:** Wraps `TcpStream` operations in dynamic buffers, allowing the server to parse and process heavily pipelined requests (e.g., 32+ commands per TCP frame) while executing only a single OS `write` syscall per event loop iteration.
* **TCP Optimization:** Explicitly disables Nagle's algorithm (`TCP_NODELAY`) to prevent artificial kernel buffering, ensuring microsecond latency on small command payloads.
* **Defensive Parsing:** The RESP parser handles TCP fragmentation natively, verifying payload boundaries before advancing buffer cursors to prevent out-of-bounds panics on incomplete network reads.

## Performance

Walrus has been extensively profiled using `perf` and flamegraphs to eliminate kernel-level blocking. In pipelined benchmarks, Walrus achieves exceptional throughput on standard hardware:

**redis-benchmark -n 5000000 -t get,set,lpop,lrange -c 50 -q -P 32 --threads 1**
* `SET`: 2551020.25 requests per second, p50=0.311 msec
* `GET`: 2682403.50 requests per second, p50=0.303 msec
* `LPOP`: 3536067.75 requests per second, p50=0.231 msec
* `LPUSH`: 3410641.25 requests per second, p50=0.239 msec
* `LRANGE_100 (first 100 elements)`: 1949317.75 requests per second, p50=0.415 msec
* `LRANGE_300 (first 300 elements)`: 1937233.62 requests per second, p50=0.415 msec
* `LRANGE_500 (first 500 elements)`: 1943256.88 requests per second, p50=0.415 msec
* `LRANGE_600 (first 600 elements)`: 1929756.75 requests per second, p50=0.415 msec

**redis-benchmark -n 5000000 -t get,set,lpop,lrange -c 50 -q -P 32 --threads 12**
* `SET`: 1426126.62 requests per second, p50=0.447 msec
* `GET`: 1426126.62 requests per second, p50=0.439 msec
* `LPOP`: 1425313.62 requests per second, p50=0.439 msec
* `LPUSH (needed to benchmark LRANGE)`: 2217294.75 requests per second, p50=0.359 msec
* `LRANGE_100 (first 100 elements)`: 766636.00 requests per second, p50=0.823 msec
* `LRANGE_300 (first 300 elements)`: 767224.19 requests per second, p50=0.799 msec
* `LRANGE_500 (first 500 elements)`: 767106.44 requests per second, p50=0.815 msec
* `LRANGE_600 (first 600 elements)`: 766753.56 requests per second, p50=0.807 msec

**redis-benchmark -n 1000000 -t get,set,lpop,lrange -c 50 -q --threads 12**
* `SET`: 399201.59 requests per second, p50=0.087 msec
* `GET`: 399680.25 requests per second, p50=0.087 msec
* `LPOP`: 399520.56 requests per second, p50=0.087 msec
* `LPUSH (needed to benchmark LRANGE)`: 399680.25 requests per second, p50=0.087 msec
* `LRANGE_100 (first 100 elements)`: 363240.09 requests per second, p50=0.095 msec
* `LRANGE_300 (first 300 elements)`: 363240.09 requests per second, p50=0.095 msec
* `LRANGE_500 (first 500 elements)`: 363240.09 requests per second, p50=0.095 msec
* `LRANGE_600 (first 600 elements)`: 363504.19 requests per second, p50=0.095 msec

**redis-benchmark -n 1000000 -t get,set,lpop,lrange -c 1000 -P 32 -q --threads 12**
* `SET`: 1287001.25 requests per second, p50=10.151 msec
* `GET`: 1295336.75 requests per second, p50=9.927 msec
* `LPOP`: 1949317.75 requests per second, p50=5.871 msec
* `LPUSH (needed to benchmark LRANGE)`: 1976284.62 requests per second, p50=5.527 msec
* `LRANGE_100 (first 100 elements)`: 768639.50 requests per second, p50=18.367 msec
* `LRANGE_300 (first 300 elements)`: 761035.00 requests per second, p50=18.831 msec
* `LRANGE_500 (first 500 elements)`: 761035.00 requests per second, p50=18.623 msec
* `LRANGE_600 (first 600 elements)`: 755287.00 requests per second, p50=18.895 msec

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

You can connect to Walrus using the standard Redis CLI.

```bash
redis-cli -h 127.0.0.1 -p 6380

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

To run integration benchmarks, use `redis-benchmark`:

```bash
redis-benchmark -h 127.0.0.1 -p 6380 -c 50 -n 100000 -q

```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
