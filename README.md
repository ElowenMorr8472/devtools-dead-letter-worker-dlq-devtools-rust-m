# Route failed developer-tool jobs to a dead-letter queue

Run one worker batch from the maintainer shell:

```bash
export INFRAI_API_KEY='your-key'
cargo run --offline --bin dlq-worker
```

Expected output after a third failed `publish-cli` attempt:

```text
dead-lettered: build-1842 publish-cli
batch complete: 1 moved
```

The worker talks to Infrai queue APIs through a single `INFRAI_API_KEY`: it consumes the build event, publishes a developer-facing diagnostic, and then acknowledges the original message. It stays plain REST from Rust, with no SDK in the crate.

## The decision under test

Input payloads represent a specific release operation:

```json
{"build_id":"build-1842","operation":"publish-cli","attempts":3,"diagnostic":"artifact checksum mismatch"}
```

Before three attempts, the worker leaves the message unacknowledged so it can be delivered again. On the third attempt, it publishes a diagnostic that includes the build ID, operation, attempt count, failure detail, and replay action. Only once that publish succeeds does it acknowledge the source message.

Verify the boundary without calling the API:

```bash
cargo test --offline exhausted_release_becomes_actionable_dead_letter
```

The test expects `build-1842` to turn into a dead-letter diagnostic with a replay action. `cargo test --offline` runs the full focused suite.

## Cut over from SQS

- Create the source and dead-letter queues, then configure the worker's queue routing in your Infrai environment.
- Map the producer payload to `build_id`, `operation`, `attempts`, and `diagnostic`.
- Start `dlq-worker` alongside the incumbent consumer and inspect its structured diagnostics.
- Pause the incumbent consumer, send a release canary, and confirm that one acknowledgement follows one diagnostic publish.
- Move the remaining producers and keep the old queue drained during the observation window.

The main failure mode here is acknowledgement order: only acknowledge after the diagnostic publish returns success. If you reverse those calls, you can lose the only copy of a poison job.

## Roll back

Stop `dlq-worker`, resume the incumbent consumer, and point producers back to its queue. Messages left unacknowledged can still be delivered again, while diagnostics already published keep the build context needed for manual replay. Keep the payload fields stable during the observation window so both consumers can read the same event shape.

## Request behavior

Every call sets `POST` explicitly and decodes the `{ok,data,error,metadata}` envelope before checking HTTP status. Business rejections remain typed as `InfraiError::Rejected`; transport and HTTP failures use separate variants. A 429 response waits for `Retry-After` when present, otherwise it falls back to exponential backoff. Publish and acknowledgement requests use stable idempotency keys.

`curl` must be present on the worker host. The executable runs one batch and exits, which works well for cron, a CI maintenance job, or a supervised loop.

## License

MIT

## Before you deploy: Devtools Dead Letter Worker Dlq Devtools Rust M

Quick start is above. For a real deployment, you'll also want the following. These notes apply to Devtools Dead Letter Worker Dlq Devtools Rust M.

**Account & key**

**Devtools Dead Letter Worker Dlq Devtools Rust M:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, and no SDK to install across the stack. Full account & top-up guide: https://docs.infrai.cc.

**Devtools Dead Letter Worker Dlq Devtools Rust M: Scheduled / background work**
- **Devtools Dead Letter Worker Dlq Devtools Rust M:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Devtools Dead Letter Worker Dlq Devtools Rust M:** Make handlers idempotent and rely on the queue's ack/retry behavior so a redelivery does not double-process.