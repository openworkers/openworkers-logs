# OpenWorkers Logs Service

High-performance logs aggregation service for OpenWorkers platform.

## Features

- **NATS subscriber** - Consumes logs from `{worker_id}.console.{level}` topics
- **PostgreSQL storage** - Persists logs with SQLx
- **SSE streaming** - Real-time log streaming via Server-Sent Events
- **REST API** - HTTP endpoints for log retrieval

## Architecture

```
NATS (*.console.*)
  ↓
Log Service (Actix-web + SQLx)
  ↓
PostgreSQL (logs table)
  ↓
SSE Stream (GET /logs/{worker_id}/stream)
```

## API Endpoints

### Health Check

```
GET /health
```

### Get Logs

```
GET /logs/{worker_id}
```

Returns last 100 logs for a worker.

### Stream Logs (SSE)

```
GET /logs/{worker_id}/stream
```

Real-time log stream via Server-Sent Events.

## Setup

```bash
# Copy env example
cp .env.example .env

# Edit DATABASE_URL and NATS_SERVERS in .env

# Run
cargo run
```

## Environment Variables

```env
DATABASE_URL=postgres://openworkers:password@localhost:5432/openworkers
NATS_SERVERS=nats://localhost:4222
NATS_CREDENTIALS=          # Optional: base64 encoded credentials
PORT=8080
RUST_LOG=info
```

## Development

```bash
# Run in dev mode
RUST_LOG=debug cargo run

# Build release
cargo build --release
```

## NATS Message Format

**Subject**: `{worker_id}.console.{level}`
**Payload**: UTF-8 string (log message)

**Example**:

```
Subject: 550e8400-e29b-41d4-a716-446655440000.console.info
Payload: "Worker started successfully"
```

## Production

The service automatically:

- ✅ Subscribes to all worker logs (`*.console.*`)
- ✅ Parses worker_id and log level from NATS subject
- ✅ Stores logs in PostgreSQL
- ✅ Broadcasts logs to SSE clients in real-time
