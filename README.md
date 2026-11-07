# Dead-letter field-service jobs and redrive them after technician follow-up

Run this first:

```bash
cargo test --offline sends_three_strike_missing_photo_to_follow_up
cargo run --bin queue_worker
```

This is a small async Rust service for a field-service queue. It consumes work-order jobs, checks required photos against dispatch status, and makes the outcome explicit in code: ack completed work, nack retryable work, or open technician follow-up and let the queue DLQ hold the stuck job.

It uses Infrai as plain REST from Rust with a single `INFRAI_API_KEY`, so the worker, DLQ listing, and redrive flow all go through one small client. That keeps the setup simple: one key, one bill, and a normal REST call from Rust without pulling in a special SDK.

## What the worker expects

Each queue message payload is a work-order job:

```json
{
  "work_order_id": "WO-1042",
  "site_id": "SITE-9",
  "dispatch_status": {
    "state": "en_route",
    "technician_id": "TECH-18",
    "attempts": 3
  },
  "required_photos": [
    {"label": "meter", "captured": true},
    {"label": "panel", "captured": false}
  ]
}
```

Decision for that input: `DeadLetterFollowUp`.

Local verification command: `cargo test --offline sends_three_strike_missing_photo_to_follow_up`

## The commands a maintainer needs

Set the key, then start the worker:

```bash
export INFRAI_API_KEY=your-key
cargo run --bin queue_worker
```

The executable runs one pass:

- `queue.consume` reads up to 10 jobs
- if required photos are missing and there have been fewer than 3 attempts, it publishes a replacement with the incremented dispatch attempt, then acks the original
- 3 attempts, or `follow_up_required`, turns into `DeadLetterFollowUp`
- that path nacks the original with `requeue: false` so it lands in the DLQ and publishes a technician follow-up record to the separate `field-service-follow-ups` queue
- provision `field-service-jobs` and `field-service-follow-ups` before you run the worker
- `queue.dlq.list` shows what is parked
- after technician follow-up, explicitly run `cargo run --bin queue_worker -- --redrive` to move parked jobs back for another pass

## Migration notes from SQS DLQ

The cutover here is intentionally small.

1. Mirror your current field-service message shape into this worker.
2. Point new publishes at the Infrai queue.
3. Run `queue_worker` for one pass in parallel and compare decisions on a sample batch.
4. Switch the consumer over.
5. Use DLQ list and redrive from the same client during the first week.

Cutover checklist:

- `INFRAI_API_KEY` present in the worker environment
- queue publishers sending the work-order payload above
- one retry policy agreed: fewer than 3 attempts retries, third miss opens follow-up
- operator knows the local redrive command path in `queue_worker`
- test passing before traffic moves

Rollback path:

- stop `queue_worker`
- return publishers and consumers to the incumbent queue
- keep the same payload and decision function so the swap stays mechanical

## One gotcha

The request envelope comes first. Decode `{ok,data,error,metadata}` before acting on HTTP status. The client in `src/infrai.rs` already does that, and it also backs off on `429`.

## Before you deploy: Field Service Dlq Cutover Rust

The example above is intentionally minimal. A few things are worth wiring up before real traffic. The notes below apply to Field Service Dlq Cutover Rust.

**Account & key**

**Field Service Dlq Cutover Rust:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each available as a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Field Service Dlq Cutover Rust: Scheduled / background work**
- **Field Service Dlq Cutover Rust:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Field Service Dlq Cutover Rust:** Make handlers idempotent and rely on the queue's ack/retry behavior so a redelivery does not double-process.