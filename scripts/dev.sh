#!/usr/bin/env bash
# One-command dev loop: infra, seed, server, live generator, demo.
# Run inside the nix dev shell:  nix develop -c ./scripts/dev.sh
set -euo pipefail
cd "$(dirname "$0")/.."

SEED_COUNT="${SEED_COUNT:-1000000}"
RATE="${RATE:-200}"

echo "==> starting docker compose stack"
docker compose up -d --wait redpanda clickhouse-main

echo "==> building"
cargo build --release -p chlive-server -p chlive-generator
[ -d node_modules ] || npm install

have=$(curl -sf 'http://localhost:8123/?user=chlive_ro&password=chlive_ro' --data-binary 'SELECT count() FROM default.events')
if [ "$have" -lt "$SEED_COUNT" ]; then
  echo "==> seeding $((SEED_COUNT - have)) historical events through Kafka"
  ./target/release/chlive-generator seed --count $((SEED_COUNT - have))
fi

trap 'kill 0' EXIT
./target/release/chlive-server --config chlive.toml &
until curl -sf localhost:8080/healthz >/dev/null; do sleep 0.3; done

echo "==> streaming $RATE events/s"
./target/release/chlive-generator stream --rate "$RATE" &

echo "==> demo at http://localhost:5173"
npm run dev -w demo
