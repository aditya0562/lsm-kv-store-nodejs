# LSM KV Store

A network-available persistent Key/Value store built on **LSM Tree** architecture with zero external runtime dependencies.

**Tech Stack:** Node.js · TypeScript

## Features

- **5 Operations** — Put, Read, Delete, ReadKeyRange, BatchPut
- **Dual Protocol** — HTTP REST API + TCP streaming for high-throughput writes
- **Persistent** — Write-Ahead Log ensures zero data loss on crash
- **Scales Beyond RAM** — Data stored in SSTables on disk with sparse indexing and Bloom filters
- **Replication** — Primary-Backup replication with automatic reconnection *(bonus)*

## Quick Start

### Option A: Docker (recommended)

Requires [Docker](https://docs.docker.com/get-docker/).

```bash
docker compose up standalone
```

Server is ready at `http://localhost:3000`. Skip to [Usage](#usage).

### Option B: Local

Requires [Node.js](https://nodejs.org/) 20+ (includes npm).

```bash
npm install
npm run build
npm start
```

## Usage

### Put

```bash
curl -X POST http://localhost:3000/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1", "value": "Alice"}'
```

### Read

```bash
curl http://localhost:3000/get/user:1
```

### Delete

```bash
curl -X DELETE http://localhost:3000/delete/user:1
```

### ReadKeyRange

```bash
curl "http://localhost:3000/range?start=user:1&end=user:9&limit=100"
```

### BatchPut

```bash
curl -X POST http://localhost:3000/batch-put \
  -H "Content-Type: application/json" \
  -d '{
    "entries": [
      {"key": "user:1", "value": "Alice"},
      {"key": "user:2", "value": "Bob"},
      {"key": "user:3", "value": "Charlie"}
    ]
  }'
```

### TCP Streaming (high-throughput writes)

A built-in TCP client is provided for streaming writes with per-message acknowledgement and backpressure:

```typescript
import { TCPClient } from './src/server/TCPClient';

const client = new TCPClient({ host: 'localhost', port: 3001 });
await client.connect();

await client.streamPut([
  { key: 'sensor:1', value: '23.5' },
  { key: 'sensor:2', value: '18.2' },
  // ... thousands of entries
]);

await client.endStream();
```

## Replication

Primary-Backup replication is supported. Every write on the primary is forwarded to the backup in real time over a persistent TCP connection with automatic reconnection on failure.

### Docker

```bash
docker compose up primary backup
```

- **Primary** — `http://localhost:3000` (reads + writes)
- **Backup** — `http://localhost:4000` (reads only, receives replicated data)

### Local

Terminal 1 — Backup:
```bash
npm start -- --role=backup --replication-port=4002 --http-port=4000 --tcp-port=4001 --data-dir=./data-backup
```

Terminal 2 — Primary:
```bash
npm start -- --role=primary --backup-host=localhost --backup-port=4002
```

### Verify Replication

```bash
# Write to primary
curl -X POST http://localhost:3000/put \
  -H "Content-Type: application/json" \
  -d '{"key": "test", "value": "hello"}'

# Read from backup
curl http://localhost:4000/get/test
# → {"key":"test","value":"hello"}

# Check replication status
curl http://localhost:3000/replication/status
```

## Testing

### Docker

```bash
docker compose run test-api            # 34 tests — CRUD, range, batch, TCP, persistence
docker compose run test-replication    # 22 tests — replication, reconnection
```

### Local

```bash
npm run test:api                       # API + TCP integration tests
npm run test:replication               # Replication integration tests
```

## Architecture

```
                    ┌──────────────────────────────┐
     HTTP :3000 ──▶ │                              │
                    │        LSM Store             │
     TCP  :3001 ──▶ │                              │
                    └──────┬───────────┬───────────┘
                           │           │
                    ┌──────▼──┐  ┌─────▼──────┐
                    │   WAL   │  │  MemTable   │
                    │ (disk)  │  │ (sorted,    │
                    │         │  │  in-memory)  │
                    └─────────┘  └──────┬──────┘
                                        │ flush when full
                                 ┌──────▼──────┐
                                 │  SSTables    │
                                 │ (immutable   │
                                 │  sorted      │
                                 │  files)      │
                                 └──────┬──────┘
                                        │ merge when count > threshold
                                 ┌──────▼──────┐
                                 │ Compaction   │
                                 └─────────────┘
```

### Write Path

1. Entry appended to **WAL** (fsync to disk for durability)
2. Entry inserted into **MemTable** (Red-Black Tree for sorted order)
3. When MemTable exceeds 4 MB, it is frozen and a new one takes over (double buffering)
4. Frozen MemTable is flushed to an immutable **SSTable** on disk
5. If the node is a primary, the WAL entry is forwarded to the backup

### Read Path

1. Check **active MemTable** → check **immutable MemTable** (if mid-flush)
2. Check **SSTables** from newest to oldest
3. Each SSTable uses a **Bloom filter** (skip if key definitely absent) and a **sparse index** (binary search to narrow disk reads)

### Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Storage engine | LSM Tree | Efficient range queries via sorted SSTables |
| HTTP library | Native `http` module | Zero external dependencies as required |
| Key ordering | Codepoint comparison | Consistent, locale-independent byte ordering |
| Durability | Configurable sync policy | Trade latency vs durability per use case |
| MemTable flush | Size-triggered, double-buffered | No write blocking during flush |
| Compaction | Size-tiered | Simple, effective for write-heavy workloads |
| Replication | Push-based via WAL hook (DI) | LSMStore stays decoupled from replication concerns |

## Configuration

All options are passed as CLI flags:

```
--data-dir=PATH           Data directory (default: ./data)
--http-port=PORT          HTTP port (default: 3000)
--tcp-port=PORT           TCP streaming port (default: 3001)
--memtable-size=BYTES     MemTable flush threshold (default: 4194304 = 4MB)
--sync-policy=POLICY      sync | group | periodic (default: group)
--role=ROLE               standalone | primary | backup (default: standalone)
--backup-host=HOST        Backup hostname (required for primary)
--backup-port=PORT        Backup replication port (required for primary)
--replication-port=PORT   Port to listen for replication (required for backup)
```

## Project Structure

```
src/
├── index.ts                  Entry point, DI wiring
├── cli/                      CLI argument parsing
├── common/                   Config, types, data structures
├── interfaces/               Core storage interfaces (IWAL, IMemTable, IStorageEngine)
├── storage/
│   ├── LSMStore.ts           Main orchestrator
│   ├── wal/                  Write-Ahead Log with configurable sync
│   ├── memtable/             Sorted in-memory buffer (Red-Black Tree)
│   ├── sstable/              Sorted String Tables (writer, reader, bloom filter)
│   ├── manifest/             Tracks active SSTables across restarts
│   └── iterator/             K-way merge iterator for range queries
├── engine/compaction/        Background SSTable compaction
├── server/                   HTTP server, TCP server + protocol, TCP client
├── replication/              Primary-Backup replication (manager, server, protocol)
└── factory/                  Abstract factory for storage components
```
