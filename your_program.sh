#!/bin/sh
# Use the release build of mikkadb-rust
exec $(dirname $0)/target/release/mikkadb-rust "$@"
