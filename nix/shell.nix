# The dnvr dev shell: the toolchain, its env, the local stack (nix/stack.nix: Redpanda in
# Docker, ClickHouse, Kafka Connect with the ClickHouse sink, the pause server, schema, the
# server), Postgres (for the permissions provider's e2e test), the seed, the examples and scripts.
#
# Every process picks its own ports (`dnvr-state pick-port`) and publishes its address; the
# others get it through `dnvr://<process>/<key>` env refs. From outside, read them with
# `dnvr-state get <process>.<key>`, e.g. `dnvr-state get playground.url`.
{
  pkgs,
  presets,
  dnvrState,
  continuous-backup,
  clickhouse,
  libchdb,
}:
let
  # events seeded into an empty ClickHouse on `dnvr up` (scripts/seed.sql)
  seedCount = 10;

  stack =
    import ./stack.nix
      {
        inherit
          pkgs
          presets
          dnvrState
          continuous-backup
          clickhouse
          ;
      }
      {
        topics = [
          "events"
          "customers"
          "orders"
          "products"
        ];
        schemaFile = "scripts/schema.sql";
        serverConfig = "nix/chlive.dev.toml";
        cacheDir = ".dnvr/chlive-cache";
        # events also stores each message's partition, offset and timestamp (scripts/schema.sql),
        # which the server reads from the messages (nix/chlive.dev.toml)
        connectorSettings.events = {
          transforms = "kafka";
          "transforms.kafka.type" = "org.apache.kafka.connect.transforms.InsertField$Value";
          "transforms.kafka.partition.field" = "kafka_partition";
          "transforms.kafka.offset.field" = "kafka_offset";
          "transforms.kafka.timestamp.field" = "kafka_ts";
        };
      };
  inherit (stack) chClient stackEnv;

  toolchain = import ./toolchain.nix { inherit pkgs dnvrState libchdb; };

  # The tests' Postgres (the permissions provider's source): a fixed port (dnvr's preset doesn't
  # pick one; the chat stack's is 54329), the `chlive` database.
  pgPort = 54328;
  pgUrl = "postgres://postgres@127.0.0.1:${toString pgPort}/chlive";

in
{ ... }:
{
  description = "chlive: Redpanda (Docker) + ClickHouse + Kafka Connect's ClickHouse sink + pause server + chlive-server (WebSocket over chlive-engine) + Postgres (the permissions provider's tests) + examples";

  # ClickHouse and Postgres are also on PATH for ad-hoc use (Redpanda runs in Docker)
  packages = toolchain.packages ++ [
    clickhouse
    pkgs.postgresql
  ];

  env = toolchain.env // {
    SEED_COUNT = toString seedCount;
  };

  inherit (stack) prerun;
  # the stack runs as a Rex session, a tab per process (`dnvr up --runner tmux` for tmux)
  defaultRunner = "rex";

  # Start-up order follows the `dnvr://` refs in each process's `env`.
  processes = stack.processes // {
    # Seed historical events (scripts/seed.sql, produced to Kafka and ingested by the
    # sink) when ClickHouse has fewer than $SEED_COUNT, then wait until they're in.
    seed = {
      env = {
        CONNECTORS_READY = "dnvr://connectors/ready";
        CH_TCP_PORT = "dnvr://clickhouse/tcpPort";
        PROXY_URL = "dnvr://redpanda/proxyUrl";
      };
      command = ''
        cd "$DNVR_ROOT"
        count() { ${chClient ''"$CH_TCP_PORT"''} --query "SELECT count() FROM default.events"; }
        have=$(count)
        if [ "$have" -lt "$SEED_COUNT" ]; then
          echo "[seed] ClickHouse has $have events; seeding $((SEED_COUNT - have))"
          clickhouse local --param_count=$((SEED_COUNT - have)) --queries-file scripts/seed.sql \
            | ${stack.produce} events >/dev/null
          until [ "$(count)" -ge "$SEED_COUNT" ]; do sleep 1; done
          echo "[seed] ClickHouse now has $(count) events"
        else
          echo "[seed] ClickHouse already has $have events"
        fi
        dnvr-state set seeded 1
        exec tail -f /dev/null
      '';
    };

    # Postgres with logical replication, for the e2e tests of the Postgres permissions provider
    # (they make their own schemas, roles and publications). dnvr's preset: data under
    # .dnvr/postgres, `database` published once the database exists.
    postgres = {
      imports = [ presets.postgres ];
      package = pkgs.postgresql;
      # initdb refuses root: started as root (e.g. a dev VM), it runs as this user
      runAsUser = "postgres";
      database = "chlive";
      port = pgPort;
      settings = {
        wal_level = "logical";
        max_replication_slots = 10;
        max_wal_senders = 10;
      };
    };

    # As chlive_ro (snapshots as chlive_snap), against this stack. Publishes `url` (the
    # WebSocket) and `httpUrl` (/healthz, /debug, /debug/state, /debug/subscriptions) as soon as its port is picked:
    # a cold `cargo build` can outlast the consumers' wait for them, and both reconnect.
    # The rest of its config: nix/chlive.dev.toml.
    server = {
      env = {
        PAUSE_SERVER_URL = "dnvr://pause-server/url";
        CH_HTTP_URL = "dnvr://clickhouse/httpUrl";
        KAFKA_BROKERS = "dnvr://redpanda/bootstrap";
      };
      command = ''
        cd "$DNVR_ROOT"
        port=$(dnvr-state pick-port)
        dnvr-state set url "ws://127.0.0.1:$port/live"
        dnvr-state set httpUrl "http://127.0.0.1:$port"
        exec cargo run --release -p chlive-server -- --config nix/chlive.dev.toml \
          --listen "127.0.0.1:$port" --clickhouse-url "$CH_HTTP_URL" \
          --kafka "$KAFKA_BROKERS" --pause-server "$PAUSE_SERVER_URL"
      '';
    };

    # Proxies /live to the server and /kafka to Redpanda's HTTP proxy (vite.config.js: writes go
    # through the server, or straight to Kafka).
    playground = {
      env = {
        CHLIVE_URL = "dnvr://server/url";
        PROXY_URL = "dnvr://redpanda/proxyUrl";
      };
      command = ''
        cd "$DNVR_ROOT"
        [ -d node_modules ] || bun install
        port=$(dnvr-state pick-port)
        dnvr-state set url "http://localhost:$port"
        # chlive-core as WebAssembly, for the optimistic inserts
        wasm || echo "chlive-wasm didn't build: inserts aren't shown before the server has them"
        exec bun --bun run --cwd examples/playground dev --port "$port" --strictPort
      '';
    };

    # The Rust client's example: alerts on the slowest requests, per-country stats.
    # It connects (and reconnects) on its own once the server is up.
    rust-alerts = {
      env.CHLIVE_URL = "dnvr://server/url";
      command = ''
        cd "$DNVR_ROOT"
        exec cargo run --release -q -p chlive-example-rust-alerts
      '';
    };
  };

  scripts = stack.scripts // {
    pg = {
      description = "psql connected to the stack's Postgres (the `chlive` database)";
      text = ''exec ${pkgs.postgresql}/bin/psql "${pgUrl}" "$@"'';
    };
    reset-data = {
      inherit (stack.scripts.reset-data) description;
      text = ''
        ${stack.scripts.reset-data.text}
        rm -rf "''${DNVR_ROOT:?}/.dnvr/postgres"
        echo "removed Postgres data too"
      '';
    };
    wasm = {
      description = "Build chlive-wasm (chlive-core for the TS client) into client/wasm/";
      text = ''
        cd "$DNVR_ROOT"
        cargo build -p chlive-wasm --target wasm32-unknown-unknown --profile wasm
        rm -rf client/wasm
        wasm-bindgen --target web --out-dir client/wasm target/wasm32-unknown-unknown/wasm/chlive_wasm.wasm
        wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
          -o client/wasm/chlive_wasm_bg.wasm client/wasm/chlive_wasm_bg.wasm
        ls -l client/wasm/chlive_wasm_bg.wasm
      '';
    };
    tui = {
      description = "A terminal table for live queries against the stack's server, e.g. `tui \"select * from events\"`";
      runtimeInputs = [ dnvrState ];
      text = ''
        CHLIVE_URL="''${CHLIVE_URL:-$(dnvr-state get server.url)}"
        export CHLIVE_URL
        cd "$DNVR_ROOT"
        exec cargo run -q -p chlive-tui -- "$@"
      '';
    };
    e2e = {
      description = "Run the tests that need the running stack: the engine's, then the server end to end";
      runtimeInputs = [ dnvrState ];
      text = ''
        ${stackEnv}
        export CHLIVE_POSTGRES_URL="''${CHLIVE_POSTGRES_URL:-${pgUrl}}"
        cd "$DNVR_ROOT"
        # the whole output is kept (the stack's tests can be timing-sensitive: a failure's
        # details shouldn't depend on what the terminal kept)
        mkdir -p .dnvr
        set -o pipefail
        status=0
        CHLIVE_E2E=1 cargo test -p chlive-engine -p chlive-e2e "$@" 2>&1 | tee .dnvr/e2e.log || status=$?
        echo "(full output: .dnvr/e2e.log)" >&2
        exit "$status"
      '';
    };
    bench = {
      description = "Benchmark a ladder of queries through @chlive/client against the stack (docs/bench/queries.md)";
      runtimeInputs = [ dnvrState ];
      text = ''
        CHLIVE_URL="''${CHLIVE_URL:-$(dnvr-state get server.url)}"
        CH_URL="''${CH_URL:-$(dnvr-state get clickhouse.httpUrl)}"
        PROXY_URL="''${PROXY_URL:-$(dnvr-state get redpanda.proxyUrl)}"
        export CHLIVE_URL CH_URL PROXY_URL
        cd "$DNVR_ROOT"
        exec bun bench/queries.ts "$@"
      '';
    };
  };
}
