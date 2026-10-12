# The `redpanda` dnvr process: Redpanda (Kafka API + HTTP proxy) in Docker; `docker` comes from
# the host. `name` is both the container and its data volume. Reset the volume together with
# the ClickHouse that ingests it (`reset-data`): Kafka offsets and ClickHouse's _offset
# watermarks must match. Picks both ports and publishes `bootstrap`, `proxyUrl` and `topics` once
# the topics exist.
{
  lib,
  writeShellApplication,
  coreutils,
  gnugrep,
  dnvrState,
  name,
  topics,
  partitions,
}:
writeShellApplication {
  inherit name;
  runtimeInputs = [
    coreutils
    gnugrep
    dnvrState
  ];
  text = ''
    name=${name}
    kafka=$(dnvr-state pick-port)
    proxy=$(dnvr-state pick-port)
    docker rm -f "$name" >/dev/null 2>&1 || true
    # The HTTP proxy batches produce requests for 100 ms by default; disable that so a
    # single write from the playground example is acknowledged (and visible) immediately.
    # Its own Kafka client defaults to 127.0.0.1:9092, so it is pointed at the picked port.
    # Each port is published as itself, so one listener advertised as 127.0.0.1:$kafka
    # works both from the host (ClickHouse, chlive-server, tests) and inside the container
    # (the HTTP proxy's own client, rpk).
    docker run --rm --name "$name" \
      -p "127.0.0.1:$kafka:$kafka" \
      -p "127.0.0.1:$proxy:$proxy" \
      -v ${name}:/var/lib/redpanda/data \
      --entrypoint /bin/bash \
      redpandadata/redpanda:v25.2.1 -c "
        rpk redpanda config set pandaproxy_client.produce_batch_delay_ms 0 &&
        rpk redpanda config set pandaproxy_client.brokers '[{address: 127.0.0.1, port: $kafka}]' &&
        exec /entrypoint.sh redpanda start --mode dev-container --smp 1 \
          --kafka-addr 0.0.0.0:$kafka \
          --advertise-kafka-addr 127.0.0.1:$kafka \
          --pandaproxy-addr 0.0.0.0:$proxy \
          --advertise-pandaproxy-addr 127.0.0.1:$proxy" &
    pid=$!
    trap 'docker stop -t 10 "$name" >/dev/null 2>&1 || true; wait $pid 2>/dev/null || true' EXIT INT TERM

    until docker exec "$name" rpk cluster health -X "brokers=127.0.0.1:$kafka" 2>/dev/null | grep -qE 'Healthy:.+true'; do
      kill -0 $pid 2>/dev/null || { echo "[redpanda] exited before becoming ready" >&2; exit 1; }
      sleep 1
    done
    topics=(${lib.escapeShellArgs topics})
    for t in "''${topics[@]}"; do
      docker exec "$name" rpk topic create -X "brokers=127.0.0.1:$kafka" "$t" -p ${toString partitions} -r 1 >/dev/null 2>&1 || true
    done
    dnvr-state set bootstrap "127.0.0.1:$kafka"
    dnvr-state set proxyUrl "http://127.0.0.1:$proxy"
    # readiness key: topics exist
    dnvr-state set topics ${lib.concatStringsSep "," topics}
    echo "[redpanda] ready: kafka 127.0.0.1:$kafka, http proxy 127.0.0.1:$proxy"
    wait $pid
  '';
}
