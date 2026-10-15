# The dnvr dev shell: the toolchain, its env, the local stack's processes (Redpanda in Docker,
# ClickHouse, Kafka Connect with the ClickHouse sink, the pause server, schema, seed, the server,
# the examples) and scripts.
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
  topics = [
    "events"
    "customers"
    "orders"
  ];
  # partitions per topic
  partitions = 3;
  # events seeded into an empty ClickHouse on `dnvr up` (scripts/seed.sql)
  seedCount = 10;

  redpanda = pkgs.callPackage ./packages/redpanda.nix {
    inherit dnvrState topics partitions;
    name = "chlive-redpanda";
  };

  cb = continuous-backup;
  # continuous-backup's binary; `pause-server <config>` is the pause server. Its tests
  # run in its own repo.
  backup = (pkgs.callPackage "${cb}/nix/backup.nix" { }).overrideAttrs (_: {
    doCheck = false;
  });
  sinkPlugin = pkgs.callPackage "${cb}/nix/clickhouse-kafka-connect.nix" { };
  supervise = import "${cb}/nix/supervise.nix" { inherit (pkgs) lib; };

  # One sink connector per topic, into the table of the same name, exactly once: the
  # connector keeps its offsets in a KeeperMap table, which the pause server reads.
  sinkConnector = topic: {
    name = "chlive-sink-${topic}";
    stateTable = "chlive_sink_${topic}_state";
    keeperPath = "/chlive-sink/${topic}";
  };
  # ClickHouse's `hostname` and `port` are added at runtime, once it has published them.
  connectorConfig =
    topic:
    let
      c = sinkConnector topic;
    in
    pkgs.writeText "chlive-sink-${topic}.json" (
      builtins.toJSON {
        "connector.class" = "com.clickhouse.kafka.connect.ClickHouseSinkConnector";
        "tasks.max" = "1";
        topics = topic;
        topic2TableMap = "${topic}=${topic}";
        ssl = "false";
        database = "default";
        username = "default";
        password = "";
        exactlyOnce = "true";
        "consumer.override.isolation.level" = "read_committed";
        "errors.tolerance" = "all";
        "errors.deadletterqueue.topic.name" = "chlive-deadletterqueue";
        "errors.deadletterqueue.topic.replication.factor" = "1";
        "errors.deadletterqueue.context.headers.enable" = "true";
        bufferCount = "0";
        zkPath = c.keeperPath;
        zkDatabase = c.stateTable;
        "value.converter" = "org.apache.kafka.connect.json.JsonConverter";
        "value.converter.schemas.enable" = "false";
        clickhouseSettings = "async_insert=0,insert_deduplicate=1";
      }
    );
  # `bootstrap.servers` and `listeners` are appended at runtime.
  connectProperties = pkgs.writeText "chlive-connect.properties" ''
    group.id=chlive-connect
    config.storage.topic=chlive-connect-configs
    offset.storage.topic=chlive-connect-offsets
    status.storage.topic=chlive-connect-status
    config.storage.replication.factor=1
    offset.storage.replication.factor=1
    status.storage.replication.factor=1
    key.converter=org.apache.kafka.connect.storage.StringConverter
    value.converter=org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable=false
    plugin.path=${sinkPlugin}/plugins
    connector.client.config.override.policy=All
    offset.flush.interval.ms=1000
  '';
  # `listen`, `connectUrl` and `clickhouseUrl` are added at runtime.
  pauseServerConfig = pkgs.writeText "chlive-pause-server.json" (
    builtins.toJSON {
      # relative to DNVR_ROOT, where the process runs
      pausesFile = ".dnvr/pause-server/pauses.json";
      pauseTimeoutSeconds = 30;
      pipelines = map (
        topic:
        let
          c = sinkConnector topic;
        in
        {
          connector = c.name;
          database = "default";
          state_table = c.stateTable;
          keeper_path = c.keeperPath;
          table = topic;
          inherit topic partitions;
        }
      ) topics;
      timeouts = {
        clickhouseConnectSeconds = 10;
        connectConnectSeconds = 10;
        connectRequestSeconds = 30;
        connectPollSeconds = 1;
        kafkaMetadataSeconds = 10;
        kafkaCatalogReadSeconds = 15;
        kafkaTransactionSeconds = 30;
        kafkaMaxPollSeconds = 86400;
        kafkaReplayPollSeconds = 15;
        recoveryCatchupSeconds = 3600;
        controllerRetrySeconds = 15;
      };
    }
  );

  # `produce <topic> '<json>' …`, or JSON on stdin: one Kafka message per JSON value, written
  # with a plain POST to Redpanda's HTTP proxy (the same endpoint the playground uses), at
  # $PROXY_URL or else the running stack's.
  produceScript = {
    description = "Produce JSON events to a topic: `produce events '{\"user_id\": \"u1\"}'` (or JSON lines on stdin)";
    runtimeInputs = [ dnvrState ];
    text = ''
      topic="''${1:?usage: produce <topic> [json …] (or JSON lines on stdin)}"
      shift
      proxy="''${PROXY_URL:-$(dnvr-state get redpanda.proxyUrl)}"
      # every JSON value (one per argument, or any number on stdin) becomes {"value": …}
      if ! records=$(if [ "$#" -gt 0 ]; then printf '%s\n' "$@"; else cat; fi \
        | ${pkgs.jq}/bin/jq -cs '{records: map({value: .})}'); then
        echo "produce: not valid JSON; nothing was written" >&2
        exit 1
      fi
      if ! resp=$(${pkgs.curl}/bin/curl -sS --fail-with-body \
        -H 'Content-Type: application/vnd.kafka.json.v2+json' \
        --data-binary "$records" \
        "$proxy/topics/$topic"); then
        echo "produce to $topic failed: $resp" >&2
        exit 1
      fi
      # The proxy answers 200 even when records fail; each then has a Kafka error code
      # (3: unknown topic).
      printf '%s' "$resp" | ${pkgs.jq}/bin/jq -r '.offsets[]
        | if .error_code then "failed: Kafka error \(.error_code)\(if .error_code == 3 then " (unknown topic)" else "" end)"
          else "produced to partition \(.partition) at offset \(.offset)" end'
      if printf '%s' "$resp" | ${pkgs.jq}/bin/jq -e 'any(.offsets[]; .error_code)' >/dev/null; then
        exit 1
      fi
    '';
  };

  # `clickhouse client` against the local ClickHouse's native port, as the default user. It
  # brings its own config file, so ~/.clickhouse-client/config.xml (user, TLS, ...) never applies.
  chClient =
    let
      config = pkgs.writeText "chlive-clickhouse-client.xml" ''
        <config>
          <host>127.0.0.1</host>
          <user>default</user>
          <password></password>
          <secure>false</secure>
        </config>
      '';
    in
    port: "clickhouse client --config-file ${config} --port ${port}";

  # For scripts run in the shell: the running stack's addresses, in the env vars chlive-server,
  # chlive-engine, the tests and the bench read; a value already set wins.
  stackEnv = ''
    export CHLIVE_CLICKHOUSE_URL="''${CHLIVE_CLICKHOUSE_URL:-$(dnvr-state get clickhouse.httpUrl)}"
    export CHLIVE_KAFKA="''${CHLIVE_KAFKA:-$(dnvr-state get redpanda.bootstrap)}"
    export CHLIVE_PAUSE_SERVER="''${CHLIVE_PAUSE_SERVER:-$(dnvr-state get pause-server.url)}"
  '';

  toolchain = with pkgs; [
    nixfmt

    # Rust toolchain
    cargo
    rustc
    clippy
    rustfmt
    rust-analyzer

    # chdb-rust links libchdb; its bindgen needs libclang
    libchdb
    llvmPackages.libclang

    # native deps for the rdkafka crate (dynamic-linking feature)
    pkg-config
    rdkafka
    zlib

    # client / examples (Bun)
    bun

    # ClickHouse (also on PATH for ad-hoc use; Redpanda runs in Docker)
    clickhouse

    jq
    curl
    dnvrState
  ];
  toolchainEnv = {
    RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
    # chdb-rust links against these instead of downloading an engine at build time;
    # build.rs files embed CHDB_LIB_DIR as an rpath.
    CHDB_LIB_DIR = "${libchdb}/lib";
    CHDB_INCLUDE_DIR = "${libchdb}/include";
    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
  };

in
{ config, ... }:
{
  description = "chlive: Redpanda (Docker) + ClickHouse + Kafka Connect's ClickHouse sink + pause server + chlive-server (WebSocket over chlive-engine) + examples";

  packages = toolchain;

  env = toolchainEnv // {
    SEED_COUNT = toString seedCount;
  };

  # The ports of ClickHouse's embedded Keeper, which only ClickHouse itself uses (its config
  # reads them with `from_env`).
  prerun = ''
    CHLIVE_KEEPER_PORT=$(dnvr-state pick-port)
    CHLIVE_KEEPER_RAFT_PORT=$(dnvr-state pick-port)
    export CHLIVE_KEEPER_PORT CHLIVE_KEEPER_RAFT_PORT
  '';

  # Start-up order follows the `dnvr://` refs in each process's `env`.
  processes = {
    # Publishes `bootstrap`, `proxyUrl` and `topics` once the topics exist.
    redpanda.command = redpanda;

    # ClickHouse via dnvr's preset, on our pinned version (nix/packages/clickhouse.nix); it picks
    # its HTTP and native ports and publishes `host`, `httpPort`, `tcpPort` and `httpUrl`, then
    # `database` once it answers queries. Writable storage for SQL-created users (see
    # scripts/schema.sql), relative to DNVR_ROOT like the preset's other paths; and an embedded
    # Keeper, for the sink's KeeperMap state tables.
    clickhouse = {
      imports = [ presets.clickhouse ];
      package = clickhouse;
      database = "default";
      httpPort = null;
      tcpPort = null;
      dataDir = ".dnvr/clickhouse";
      timezone = "UTC";
      extraConfigXml = ''
        <access_control_path>.dnvr/clickhouse/access/</access_control_path>
        <keeper_server>
          <tcp_port from_env="CHLIVE_KEEPER_PORT"/>
          <server_id>1</server_id>
          <log_storage_path>.dnvr/clickhouse/coordination/log</log_storage_path>
          <snapshot_storage_path>.dnvr/clickhouse/coordination/snapshots</snapshot_storage_path>
          <raft_configuration>
            <server>
              <id>1</id>
              <hostname>127.0.0.1</hostname>
              <port from_env="CHLIVE_KEEPER_RAFT_PORT"/>
            </server>
          </raft_configuration>
        </keeper_server>
        <zookeeper>
          <node><host>127.0.0.1</host><port from_env="CHLIVE_KEEPER_PORT"/></node>
        </zookeeper>
        <keeper_map_path_prefix>/clickhouse/keeper_map</keeper_map_path_prefix>
      '';
    };

    # Tables, the sink's state tables and the users. Idempotent; reruns on every start
    # and then idles so its `ready` key stays live.
    schema = {
      env = {
        CH_DATABASE = "dnvr://clickhouse/database";
        CH_TCP_PORT = "dnvr://clickhouse/tcpPort";
      };
      command = ''
        cd "$DNVR_ROOT"
        ${chClient ''"$CH_TCP_PORT"''} --multiquery < scripts/schema.sql
        dnvr-state set ready 1
        echo "[schema] applied scripts/schema.sql"
        exec tail -f /dev/null
      '';
    };

    # Kafka Connect, running the ClickHouse sink connectors.
    connect = {
      env.KAFKA = "dnvr://redpanda/bootstrap";
      command = ''
        cd "$DNVR_ROOT"
        mkdir -p .dnvr/connect
        port=$(dnvr-state pick-port)
        props=.dnvr/connect/connect.properties
        {
          cat ${connectProperties}
          echo "bootstrap.servers=$KAFKA"
          echo "listeners=http://127.0.0.1:$port"
        } > "$props"
        ${supervise {
          name = "Kafka Connect";
          start = ''${pkgs.apacheKafka}/bin/connect-distributed.sh "$props"'';
          readyWhen = ''${pkgs.curl}/bin/curl -fs "http://127.0.0.1:$port/connector-plugins" >/dev/null'';
          onReady = ''
            dnvr-state set url "http://127.0.0.1:$port"
            echo "[connect] ready"
          '';
        }}
      '';
    };

    # One sink connector per topic (created or updated), once the tables exist.
    connectors = {
      env = {
        CONNECT_URL = "dnvr://connect/url";
        SCHEMA_READY = "dnvr://schema/ready";
        TOPICS = "dnvr://redpanda/topics";
        CH_HOST = "dnvr://clickhouse/host";
        CH_HTTP_PORT = "dnvr://clickhouse/httpPort";
      };
      command = ''
        ${pkgs.lib.concatMapStrings (topic: ''
          ${pkgs.jq}/bin/jq --arg host "$CH_HOST" --arg port "$CH_HTTP_PORT" \
            '. + {hostname: $host, port: $port}' ${connectorConfig topic} \
            | ${pkgs.curl}/bin/curl -sS --fail-with-body -X PUT \
              -H 'Content-Type: application/json' --data-binary @- \
              "$CONNECT_URL/connectors/${(sinkConnector topic).name}/config" >/dev/null
          # a stop during a snapshot leaves the sink paused (the pause server's pause outlives
          # it), and a task that failed on old settings stays failed: start clean
          ${pkgs.curl}/bin/curl -sS --fail-with-body -X PUT \
            "$CONNECT_URL/connectors/${(sinkConnector topic).name}/resume" >/dev/null
          ${pkgs.curl}/bin/curl -sS --fail-with-body -X POST \
            "$CONNECT_URL/connectors/${(sinkConnector topic).name}/restart?includeTasks=true&onlyFailed=true" >/dev/null
          until ${pkgs.curl}/bin/curl -fs "$CONNECT_URL/connectors/${(sinkConnector topic).name}/status" \
            | ${pkgs.jq}/bin/jq -e '.connector.state == "RUNNING" and (.tasks | length > 0) and all(.tasks[]; .state == "RUNNING")' >/dev/null; do
            sleep 0.5
          done
          echo "[connectors] ${(sinkConnector topic).name} running"
        '') topics}
        dnvr-state set ready 1
        exec tail -f /dev/null
      '';
    };

    # Pauses a table's ingestion while chlive-server snapshots it.
    pause-server = {
      env = {
        CONNECTORS_READY = "dnvr://connectors/ready";
        CONNECT_URL = "dnvr://connect/url";
        CLICKHOUSE_URL = "dnvr://clickhouse/httpUrl";
      };
      command = ''
        cd "$DNVR_ROOT"
        mkdir -p .dnvr/pause-server
        port=$(dnvr-state pick-port)
        conf=.dnvr/pause-server/config.json
        ${pkgs.jq}/bin/jq --arg listen "127.0.0.1:$port" \
          --arg connect "$CONNECT_URL" --arg clickhouse "$CLICKHOUSE_URL" \
          '. + {listen: $listen, connectUrl: $connect, clickhouseUrl: $clickhouse}' \
          ${pauseServerConfig} > "$conf"
        export CLICKHOUSE_USERNAME=default CLICKHOUSE_PASSWORD=
        ${supervise {
          name = "Pause server";
          start = ''${backup}/bin/durable-clickhouse-backup pause-server "$conf"'';
          readyWhen = ''${pkgs.curl}/bin/curl -fs "http://127.0.0.1:$port/health" >/dev/null'';
          onReady = ''
            dnvr-state set url "http://127.0.0.1:$port"
            echo "[pause-server] ready"
          '';
        }}
      '';
    };

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
            | ${
              pkgs.lib.getExe (
                pkgs.writeShellApplication {
                  name = "produce";
                  inherit (produceScript) text runtimeInputs;
                }
              )
            } events >/dev/null
          until [ "$(count)" -ge "$SEED_COUNT" ]; do sleep 1; done
          echo "[seed] ClickHouse now has $(count) events"
        else
          echo "[seed] ClickHouse already has $have events"
        fi
        dnvr-state set seeded 1
        exec tail -f /dev/null
      '';
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

    # Proxies /live to the server and /kafka to Redpanda's HTTP proxy (vite.config.js).
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

  scripts = {
    produce = produceScript;
    ch = {
      description = "clickhouse client connected to the local ClickHouse";
      runtimeInputs = [ dnvrState ];
      text = ''exec ${chClient ''"$(dnvr-state get clickhouse.tcpPort)"''} "$@"'';
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
    reset-data = {
      description = "Delete local Redpanda, ClickHouse and chlive cache data (run while `dnvr up` is stopped)";
      text = ''
        # Kafka (with Connect's own topics) and ClickHouse (with the sink's KeeperMap
        # offsets) must be reset together; chlive's stored results hold cursors into them.
        docker rm -f chlive-redpanda >/dev/null 2>&1 || true
        docker volume rm chlive-redpanda >/dev/null 2>&1 || true
        rm -rf "''${DNVR_ROOT:?}/${config.processes.clickhouse.dataDir}"
        rm -rf "''${DNVR_ROOT:?}/.dnvr/chlive-cache"
        echo "removed Redpanda, ClickHouse and chlive cache data; next \`dnvr up\` starts empty and re-seeds"
      '';
    };
  };
}
