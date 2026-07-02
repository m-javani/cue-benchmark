# 📊 CUE Benchmark

## What This Benchmark Measures

This is **not** a standard micro-benchmark or tail-latency test. It measures the **end-to-end throughput** of the CUE system in two distinct phases:

### Phase 1: **Ingestion** (Producer → Proxy → Cluster)
- How fast can the system **accept** jobs from producers?
- Measures: Jobs accepted/second, HTTP throughput
- This tests the write path and WAL performance

### Phase 2: **Dispatch** (Cluster → Proxy → Consumer)
- How fast can the system **deliver** jobs to consumers?
- Measures: Jobs delivered/second, WebSocket throughput
- This tests the read path and stream delivery

### Why Two Phases?
CUE decouples ingestion from dispatch. Jobs can be accepted quickly (Phase 1) while consumers process them asynchronously (Phase 2). This benchmark measures **both** capabilities independently.

---

## Requirements

- cue-proxy v0.2.0 or later
- cue v0.2.0 or later

> both should be the same version

## Prerequisites

Before running the benchmark, you need:

1. **A running CUE cluster**
2. **A single CUE proxy** connected to the cluster

The benchmark will **not** start these services for you.

---

## Installation

```bash
# Clone the repository
git clone https://github.com/m-javani/cue-benchmark
cd cue-benchmark

# Run directly (no build needed)
go run main.go --help

# Or Build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -extldflags '-static'" -o cue-benchmark .

chmod +x ./cue-benchmark

```

---

## Usage

### Basic Example
```bash
./cue-benchmark \
  -topics 4 \
  -consumers 200 \
  -workers 100 \
  -jobs 4000 \
  -proxy http://localhost:8080
```

### Full Options
```bash
./cue-benchmark \
  -proxy http://localhost:8080 \
  -proxy-token admin_2024 \
  -consumer-token admin_2024 \
  -topics 4 \
  -jobs 10000 \
  -consumers 200 \
  -workers 100 \
  -payload 1024 \
  -timeout 60s \
  -insecure false \
  -log-level info
```

---

## Command Line Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `-proxy` | `http://localhost:8080` | CUE proxy endpoint URL |
| `-proxy-token` | `admin_2024` | Authentication token for producer API |
| `-consumer-token` | `admin_2024` | Authentication token for WebSocket consumers |
| `-topics` | `1` | Number of topics to create. Jobs are distributed round-robin across topics |
| `-jobs` | `1000` | **Total** jobs to submit. Each topic receives `jobs/topics` jobs |
| `-consumers` | `1` | Number of WebSocket consumers. Distributed round-robin across topics |
| `-workers` | `64` | Concurrent HTTP workers for job submission. Higher = more aggressive ingestion |
| `-payload` | `1024` | Payload size in bytes per job (base64-encoded, actual JSON size slightly larger) |
| `-timeout` | `60s` | Maximum time to wait for all jobs to be dispatched |
| `-insecure` | `false` | Skip TLS certificate verification (for self-signed certs) |
| `-log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |

---

## How It Works

### Phase 1: Ingestion (Producer → System)

1. **Creates topics** - One HTTP request per topic
2. **Submits jobs** - `-jobs` total jobs distributed round-robin across topics
3. **Measures**:
   - Total jobs accepted (HTTP 200 OK)
   - Ingestion duration (time from first to last accepted job)
   - Ingestion throughput (jobs/sec)

### Phase 2: Dispatch (System → Consumers)

1. **Connects consumers** - `-consumers` WebSocket connections established
2. **Consumers wait** - Each consumer subscribes to one topic (round-robin)
3. **Measures**:
   - Total jobs received by consumers
   - Dispatch duration (time from when all consumers are ready until last job received)
   - Dispatch throughput (jobs/sec)

### What's NOT Measured
- **Per-request latency** (p50, p95, p99) - This is a throughput test
- **Job processing time** - Consumers acknowledge immediately
- **End-to-end** (from produce to consume) - Phases are measured separately

---

## Interpreting Results

### Sample Output
```
==================================================
PHASE 1 — INGESTION
Jobs Accepted : 4000
Duration      : 1.234s
Throughput    : 3241.49 jobs/sec
--------------------------------------------------
PHASE 2 — DISPATCH
Jobs Delivered: 4000
Duration      : 2.456s
Throughput    : 1628.66 jobs/sec
--------------------------------------------------
Errors          : 0
HTTP Errors     : 0
WebSocket Errors: 0
==================================================
```

### What These Numbers Mean

| Metric | What It Tells You |
|--------|-------------------|
| **Ingestion Throughput** | How fast the system can accept new work. Limited by proxy HTTP handling, cluster Raft consensus, WAL writes |
| **Dispatch Throughput** | How fast the system can deliver work to consumers. Limited by proxy WebSocket fan-out, cluster stream replication |
| **Errors** | Any failures (should be 0). Non-zero indicates system instability |
| **Phase Ratio** | If dispatch is slower than ingestion, consumers are the bottleneck |


---

## Tuning Recommendations

### For Ingestion Performance
- Increase `-workers` to saturate CPU (start with 100, tune up)
- Use smaller `-payload` to reduce network/encoding overhead
- Ensure proxy has sufficient CPU (2+ cores)

### For Dispatch Performance
- Match `-consumers` to expected application scale
- More consumers generally = higher dispatch throughput
- Proxy WebSocket handling benefits from multi-core

### Finding System Limits
```bash
# Find max ingestion throughput
./cue-benchmark -jobs 100000 -workers 500 -consumers 0  # No consumers

# Find max dispatch throughput (pre-fill cluster first)
./cue-benchmark -jobs 100000 -workers 1 -consumers 500  # Slow produce, fast consume

# Find balanced throughput
./cue-benchmark -jobs 100000 -workers 100 -consumers 200
```

---

## Troubleshooting

### "timeout: X/Y jobs received"
**Cause**: Dispatch is slower than expected
**Fix**: Increase `-timeout` or reduce `-jobs`/`-consumers`

### High HTTP Errors
**Cause**: Proxy overload or network issues
**Fix**: Reduce `-workers`, check proxy CPU/memory, verify cluster health

### High WebSocket Errors
**Cause**: Consumer connections failing
**Fix**: Check consumer token, proxy WebSocket limit, network connectivity

### "add topic failed"
**Cause**: Topic already exists (ignore) or cluster not ready
**Fix**: Ensure cluster is running before benchmark starts

---

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

---

## Contributing

Found a bug? Want to add a new scenario? See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Related Repositories

- [cue](https://github.com/m-javani/cue) - Core cluster
- [cue-proxy](https://github.com/m-javani/cue-proxy) - HTTP/WebSocket gateway
- [cue-docs](https://m-javani.github.io/cue-docs/) - Documentation