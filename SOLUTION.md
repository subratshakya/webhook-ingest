# Webhook Ingestion Defects Solution

This document outlines the solutions implemented to address the production defects observed in the `webhook-ingest` application.

## 1. What was broken and why

- **Duplicate call records & higher account call counts:**
  - *Cause:* The `ingest` service was using an application-level check-then-insert approach (`EventExists` followed by `InsertEvent` and `UpsertCall`). Under concurrent requests (e.g., rapid redeliveries of the same event), multiple threads read `exists = false` simultaneously and proceeded to insert multiple events and increment account stats concurrently.
  - *Fix:* Shifted the deduplication logic to the database layer. A `UNIQUE` constraint was added to the `event_id` column. Event insertion and related updates are now encapsulated in a single atomic transaction. We use `INSERT ... ON CONFLICT (event_id) DO NOTHING` to gracefully catch and ignore duplicate redeliveries.
  
- **Stats cache data races:**
  - *Cause:* The in-memory stats cache (`internal/stats/cache.go`) lacked a write lock in the `Record` method, causing concurrent updates to the internal map (`c.m`) to race and potentially panic or lose data.
  - *Fix:* Added `c.mu.Lock()` to `Record`.

- **Recordings not marked processed & failures not logged:**
  - *Cause:* The goroutine responsible for processing recordings (`processRecording`) was handed the HTTP request's context. When the HTTP handler returned early to acknowledge the webhook, its context was canceled, causing the database update (`MarkRecordingProcessed`) to abort with a context cancellation error. Additionally, errors returned by `processRecording` were silently ignored.
  - *Fix:* The goroutine now uses `context.Background()`, detaching it from the HTTP request lifecycle. We also added logging to capture any processing failures.

- **In-flight work disappears on deployment:**
  - *Cause:* When the server shuts down, any ongoing recording processing goroutines are killed, and the database `calls` record remains forever stuck with `recording_processed = FALSE`.
  - *Fix:* Implemented a recovery mechanism. On startup, the server fetches all `call_id`s that have an unprocessed recording URL and spawns background goroutines to finish processing them.

## 2. Deduplication Strategy

**Chosen Strategy:** Database-enforced UNIQUE constraint + atomic Postgres transaction.

**Why this was chosen:**
- *Strong guarantees:* A database UNIQUE constraint guarantees idempotency even if multiple application instances are running or restarting. Application-level check-then-insert is fundamentally vulnerable to race conditions unless distributed locks (like Redis) are used.
- *Atomicity:* Postgres transactions ensure that we either insert the event, update the call record, and increment stats completely, or we don't do it at all.
- *Simplicity & Existing Architecture:* The application already uses Postgres as the source of truth. Using Postgres for deduplication avoids introducing additional state or failure modes that a Redis-based distributed lock might bring.

## 3. Scaling to 10,000 webhooks/second

To support 10,000 webhooks per second, the following changes would be necessary:
- **Asynchronous Processing Queue:** Currently, webhooks are written synchronously to the database in the HTTP handler. At 10k RPS, the database connection pool and transaction overhead would become a major bottleneck. We should accept the webhook, push the payload to a high-throughput message queue (like Kafka, RabbitMQ, or AWS SQS), and return HTTP 200 immediately. Dedicated worker nodes would then consume from the queue and perform the database inserts/updates in batches.
- **Database Partitioning & Indexing:** At 10k RPS, the `events` and `calls` tables will grow extremely fast. We'd need to implement table partitioning (e.g., by date) and ensure indexes are highly optimized.
- **Redis Stats Update:** The current setup updates both Redis and Postgres synchronously per request. We might shift to primarily incrementing counters in Redis using fast atomic operations (`INCRBY`), and occasionally flushing aggregates to Postgres.
- **Recording Processing Scalability:** Goroutines spawned per request will consume too much memory and CPU if they perform heavy downloading/transcoding. This work should definitely be moved to dedicated consumer workers picking jobs off a queue.
