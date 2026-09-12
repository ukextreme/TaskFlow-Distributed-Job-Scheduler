# TaskFlow

A distributed asynchronous job scheduler in Rust.

This repository holds the **minimal scheduler core**: a priority queue of jobs, a condvar-driven
worker pool, and bounded retries with requeue-on-failure. No external crates — `std` only.

```bash
cargo run --release
```

Jobs are ordered by priority first and FIFO within a priority. A failing job is requeued with an
incremented attempt count until `max_attempts`, then dropped to the failure counter.

## Status

Core only. Transport (MQTT/gRPC), the topic registry, and the container/observability setup are
not part of this repository.
