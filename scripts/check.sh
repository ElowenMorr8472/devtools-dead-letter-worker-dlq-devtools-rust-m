#!/bin/sh
set -eu
cargo test --offline
cargo run --offline --bin dlq-worker

