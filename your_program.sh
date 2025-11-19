#!/bin/sh
# exec cargo run --release --manifest-path $(dirname $0)/Cargo.toml -- "$@"
exec ./target/debug/mikkadb-rust "$@"
