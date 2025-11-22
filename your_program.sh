#!/bin/sh
# Use the release build with single-lock (RwLock) implementation
exec $(dirname $0)/target/release/bench-single-lock "$@"
