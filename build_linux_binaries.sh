#!/bin/bash
set -e

echo "Building baseline binary for Linux..."
git stash
cargo build --release --target x86_64-unknown-linux-gnu
cp target/x86_64-unknown-linux-gnu/release/redis-starter-rust mikkadb-baseline-linux
git stash pop

echo ""
echo "Building adaptive buffering binary for Linux..."
cargo build --release --target x86_64-unknown-linux-gnu
cp target/x86_64-unknown-linux-gnu/release/redis-starter-rust mikkadb-adaptive-linux

echo ""
echo "Binaries built:"
ls -lh mikkadb-*-linux
