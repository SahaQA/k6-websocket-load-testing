# WebSocket Chat Server — Load Testing with k6

A WebSocket-based messaging server where users are auto-paired for 1:1 chat, with k6 load test scripts for performance validation.

## Architecture

```
┌──────────┐    WebSocket     ┌──────────────────────┐    WebSocket     ┌──────────┐
│  User A  │ ──────────────── │   server.js (ws)     │ ──────────────── │  User B  │
│ (k6 VU)  │  register/msg    │                      │  register/msg    │ (k6 VU)  │
└──────────┘                  │  • Auto-pairing queue│                  └──────────┘
                              │  • Message routing   │
                              │  • Ack delivery      │
                              └──────────────────────┘
```

## Protocol

| Action   | Client sends                                         | Server responds                          |
|----------|------------------------------------------------------|------------------------------------------|
| Register | `{"type":"register","userId":"user1"}`               | `{"type":"waiting"}` or `{"type":"paired","partnerId":"user2"}` |
| Message  | `{"type":"message","content":"hi","seq":1}`          | `{"type":"ack","seq":1}` to sender; `{"type":"message","from":"user1","content":"hi","seq":1}` to partner |

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Start the server

```bash
node server.js
# => WebSocket Chat Server running on ws://localhost:8080
```

### 3. Run k6 load tests

**Scenario 1 — 2 VUs (1 pair), 100 messages in 500ms:**

```bash
k6 run k6-test-2vu.js
```

**Scenario 2 — 10 VUs (5 pairs), 100 messages per pair in 500ms:**

```bash
k6 run k6-test-10vu.js
```

**Custom WebSocket URL:**

```bash
k6 run -e WS_URL=ws://your-server:8080 k6-test-10vu.js
```

### 4. Alternative: Node.js load test (no k6 required)

```bash
# 2 VUs
VUS=2 node loadtest.js

# 10 VUs
VUS=10 node loadtest.js
```

## Files

| File              | Description                                              |
|-------------------|----------------------------------------------------------|
| `server.js`       | WebSocket chat server with auto-pairing and message routing |
| `k6-test-2vu.js`  | k6 script: 2 VUs, 1 pair, 100 msgs in 500ms             |
| `k6-test-10vu.js` | k6 script: 10 VUs, 5 pairs, 100 msgs/pair in 500ms      |
| `loadtest.js`     | Node.js equivalent load test (same logic, no k6 needed)  |

## Test Thresholds

### 2-VU Test
- Messages exchanged >= 100
- p95 latency < 50ms
- Pairing time p95 < 2s

### 10-VU Test
- Messages exchanged >= 500 (5 pairs × 100)
- p95 latency < 100ms
- Pairing time p95 < 5s
- Pair success rate > 90%

## Metrics Collected

- **messages_exchanged** — total acknowledged messages
- **message_latency_ms** — round-trip time from send to ack
- **pairing_time_ms** — time from connection to being paired
- **pair_success** — rate of successful pair formations (10-VU test)

## Installing k6

```bash
# macOS
brew install k6

# Ubuntu/Debian
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
  --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D68
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6

# Windows
choco install k6
# or
winget install k6 --source winget

# Docker
docker run --rm -i grafana/k6 run - <k6-test-2vu.js
```
