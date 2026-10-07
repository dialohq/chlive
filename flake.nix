{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    dnvr.url = "github:dialohq/dnvr";
    dnvr.inputs.nixpkgs.follows = "nixpkgs";
    # The production ingestion (Kafka Connect's ClickHouse sink, exactly once) and its pause
    # server, which chlive's snapshots go through. A local checkout for now.
    continuous-backup = {
      url = "git+file:///Users/adam/continuous-backup";
      flake = false;
    };
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      imports = [ inputs.dnvr.flakeModule ];

      perSystem =
        {
          pkgs,
          presets,
          dnvrState,
          ...
        }:
        let
          # Ports and names shared by the dnvr processes below. They are also the built-in defaults
          # of chlive-server, chlive-engine, the tests, scripts/schema.sql and
          # examples/playground/vite.config.js, which must agree with them.
          settings = {
            kafkaPort = 9092; # Redpanda Kafka API (its standard port), published to the host
            pandaproxyPort = 8082; # Redpanda HTTP proxy (its standard port; the example's writes)
            clickhouse = {
              # 18123/19000 rather than 8123/9000, so it can run next to another local ClickHouse
              httpPort = 18123;
              tcpPort = 19000; # native protocol (`ch`)
            };
            playgroundPort = 5174;
            connectPort = 18083; # Kafka Connect's REST API
            pauseServerPort = 18091; # the pause server (snapshots)
            keeperPort = 19181; # ClickHouse Keeper, embedded (KeeperMap: the sink's state)
            keeperRaftPort = 19234;
            topics = [
              "events"
              "customers"
              "orders"
            ];
            # events seeded into an empty ClickHouse on `dnvr up` (scripts/seed.sql)
            seedCount = 10;
            # partitions per topic (nix/packages/redpanda.nix creates them so)
            partitions = 3;
          };

          cb = inputs.continuous-backup;
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
                hostname = "127.0.0.1";
                port = toString settings.clickhouse.httpPort;
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
          connectProperties = pkgs.writeText "chlive-connect.properties" ''
            bootstrap.servers=127.0.0.1:${toString settings.kafkaPort}
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
            listeners=http://127.0.0.1:${toString settings.connectPort}
            connector.client.config.override.policy=All
            offset.flush.interval.ms=1000
          '';
          pauseServerConfig = pkgs.writeText "chlive-pause-server.json" (
            builtins.toJSON {
              listen = "127.0.0.1:${toString settings.pauseServerPort}";
              # relative to DNVR_ROOT, where the process runs
              pausesFile = ".dnvr/pause-server/pauses.json";
              connectUrl = "http://127.0.0.1:${toString settings.connectPort}";
              clickhouseUrl = "http://127.0.0.1:${toString settings.clickhouse.httpPort}";
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
                  inherit topic;
                  inherit (settings) partitions;
                }
              ) settings.topics;
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

          clickhouse = pkgs.callPackage ./nix/packages/clickhouse.nix { };
          # chDB (ClickHouse as a library), embedded in the engine to merge aggregate states;
          # built from the same ClickHouse version as ./clickhouse.nix.
          libchdb = pkgs.callPackage ./nix/packages/libchdb.nix { };
          redpanda = pkgs.callPackage ./nix/packages/redpanda.nix {
            inherit dnvrState;
            name = "chlive-redpanda";
            inherit (settings) kafkaPort pandaproxyPort topics;
          };

          # `produce <topic> '<json>' …`, or JSON on stdin: one Kafka message per JSON value, written
          # with a plain POST to Redpanda's HTTP proxy (the same endpoint the playground uses).
          produceScript = {
            description = "Produce JSON events to a topic: `produce events '{\"user_id\": \"u1\"}'` (or JSON lines on stdin)";
            text = ''
              topic="''${1:?usage: produce <topic> [json …] (or JSON lines on stdin)}"
              shift
              # every JSON value (one per argument, or any number on stdin) becomes {"value": …}
              if ! records=$(if [ "$#" -gt 0 ]; then printf '%s\n' "$@"; else cat; fi \
                | ${pkgs.jq}/bin/jq -cs '{records: map({value: .})}'); then
                echo "produce: not valid JSON; nothing was written" >&2
                exit 1
              fi
              if ! resp=$(${pkgs.curl}/bin/curl -sS --fail-with-body \
                -H 'Content-Type: application/vnd.kafka.json.v2+json' \
                --data-binary "$records" \
                "http://127.0.0.1:${toString settings.pandaproxyPort}/topics/$topic"); then
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

          # `clickhouse client` against the local ClickHouse, as the default user. It brings its
          # own config file, so ~/.clickhouse-client/config.xml (user, TLS, ...) never applies.
          chClient =
            let
              config = pkgs.writeText "chlive-clickhouse-client.xml" ''
                <config>
                  <host>127.0.0.1</host>
                  <port>${toString settings.clickhouse.tcpPort}</port>
                  <user>default</user>
                  <password></password>
                  <secure>false</secure>
                </config>
              '';
            in
            "clickhouse client --config-file ${config}";

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
        {
          formatter = pkgs.nixfmt;
          packages = {
            inherit clickhouse redpanda libchdb;
          };

          dnvr.shells.default =
            { config, ... }:
            {
              description = "chlive: Redpanda (Docker) + ClickHouse + Kafka Connect's ClickHouse sink + pause server + chlive-server (WebSocket over chlive-engine) + examples";

              packages = toolchain;

              # The server, the engine CLI and the tests default to this stack's ports; set
              # CHLIVE_CLICKHOUSE_URL / CHLIVE_KAFKA / CHLIVE_LISTEN to point them elsewhere.
              env = toolchainEnv // {
                SEED_COUNT = toString settings.seedCount;
              };

              # Start-up order follows the `dnvr://` refs in each process's `env`.
              processes = {
                redpanda.command = redpanda;

                # ClickHouse via dnvr's preset, on our pinned version (nix/packages/clickhouse.nix).
                # Writable storage for SQL-created users (see scripts/schema.sql), relative to
                # DNVR_ROOT like the preset's other paths; and an embedded Keeper, for the
                # sink's KeeperMap state tables.
                clickhouse = {
                  imports = [ presets.clickhouse ];
                  package = clickhouse;
                  database = "default";
                  inherit (settings.clickhouse) httpPort tcpPort;
                  dataDir = ".dnvr/clickhouse";
                  timezone = "UTC";
                  extraConfigXml = ''
                    <access_control_path>.dnvr/clickhouse/access/</access_control_path>
                    <keeper_server>
                      <tcp_port>${toString settings.keeperPort}</tcp_port>
                      <server_id>1</server_id>
                      <log_storage_path>.dnvr/clickhouse/coordination/log</log_storage_path>
                      <snapshot_storage_path>.dnvr/clickhouse/coordination/snapshots</snapshot_storage_path>
                      <raft_configuration>
                        <server>
                          <id>1</id>
                          <hostname>127.0.0.1</hostname>
                          <port>${toString settings.keeperRaftPort}</port>
                        </server>
                      </raft_configuration>
                    </keeper_server>
                    <zookeeper>
                      <node><host>127.0.0.1</host><port>${toString settings.keeperPort}</port></node>
                    </zookeeper>
                    <keeper_map_path_prefix>/clickhouse/keeper_map</keeper_map_path_prefix>
                  '';
                };

                # Tables, the sink's state tables and the users. Idempotent; reruns on every start
                # and then idles so its `ready` key stays live.
                schema = {
                  env.CH_DATABASE = "dnvr://clickhouse/database";
                  command = ''
                    cd "$DNVR_ROOT"
                    ${chClient} --multiquery < scripts/schema.sql
                    dnvr-state set ready 1
                    echo "[schema] applied scripts/schema.sql"
                    exec tail -f /dev/null
                  '';
                };

                # Kafka Connect, running the ClickHouse sink connectors.
                connect = {
                  env.KAFKA = "dnvr://redpanda/bootstrap";
                  command = ''
                    ${supervise {
                      name = "Kafka Connect";
                      start = "${pkgs.apacheKafka}/bin/connect-distributed.sh ${connectProperties}";
                      readyWhen = "${pkgs.curl}/bin/curl -fs http://127.0.0.1:${toString settings.connectPort}/connector-plugins >/dev/null";
                      onReady = ''
                        dnvr-state set url http://127.0.0.1:${toString settings.connectPort}
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
                  };
                  command = ''
                    ${pkgs.lib.concatMapStrings (topic: ''
                      ${pkgs.curl}/bin/curl -sS --fail-with-body -X PUT \
                        -H 'Content-Type: application/json' --data-binary @${connectorConfig topic} \
                        "$CONNECT_URL/connectors/${(sinkConnector topic).name}/config" >/dev/null
                      until ${pkgs.curl}/bin/curl -fs "$CONNECT_URL/connectors/${(sinkConnector topic).name}/status" \
                        | ${pkgs.jq}/bin/jq -e '.connector.state == "RUNNING" and (.tasks | length > 0) and all(.tasks[]; .state == "RUNNING")' >/dev/null; do
                        sleep 0.5
                      done
                      echo "[connectors] ${(sinkConnector topic).name} running"
                    '') settings.topics}
                    dnvr-state set ready 1
                    exec tail -f /dev/null
                  '';
                };

                # Pauses a table's ingestion while chlive-server snapshots it.
                pause-server = {
                  env.CONNECTORS_READY = "dnvr://connectors/ready";
                  command = ''
                    cd "$DNVR_ROOT"
                    mkdir -p .dnvr/pause-server
                    export CLICKHOUSE_USERNAME=default CLICKHOUSE_PASSWORD=
                    ${supervise {
                      name = "Pause server";
                      start = "${backup}/bin/durable-clickhouse-backup pause-server ${pauseServerConfig}";
                      readyWhen = "${pkgs.curl}/bin/curl -fs http://127.0.0.1:${toString settings.pauseServerPort}/health >/dev/null";
                      onReady = ''
                        dnvr-state set url http://127.0.0.1:${toString settings.pauseServerPort}
                        echo "[pause-server] ready"
                      '';
                    }}
                  '';
                };

                # Seed historical events (scripts/seed.sql, produced to Kafka and ingested by the
                # sink) when ClickHouse has fewer than $SEED_COUNT, then wait until they're in.
                seed = {
                  env.CONNECTORS_READY = "dnvr://connectors/ready";
                  command = ''
                    cd "$DNVR_ROOT"
                    count() { ${chClient} --query "SELECT count() FROM default.events"; }
                    have=$(count)
                    if [ "$have" -lt "$SEED_COUNT" ]; then
                      echo "[seed] ClickHouse has $have events; seeding $((SEED_COUNT - have))"
                      clickhouse local --param_count=$((SEED_COUNT - have)) --queries-file scripts/seed.sql \
                        | ${pkgs.lib.getExe (pkgs.writeShellApplication { name = "produce"; inherit (produceScript) text; })} events >/dev/null
                      until [ "$(count)" -ge "$SEED_COUNT" ]; do sleep 1; done
                      echo "[seed] ClickHouse now has $(count) events"
                    else
                      echo "[seed] ClickHouse already has $have events"
                    fi
                    dnvr-state set seeded 1
                    exec tail -f /dev/null
                  '';
                };

                # On its defaults: :8080, this ClickHouse (as chlive_ro, snapshots as
                # chlive_snap), this Redpanda and this pause server.
                server = {
                  env.PAUSE_SERVER = "dnvr://pause-server/url";
                  command = ''
                    cd "$DNVR_ROOT"
                    exec cargo run --release -p chlive-server
                  '';
                };

                playground = {
                  command = ''
                    cd "$DNVR_ROOT"
                    [ -d node_modules ] || bun install
                    exec bun --bun run --cwd examples/playground dev --port ${toString settings.playgroundPort} --strictPort
                  '';
                };
              };

              scripts = {
                produce = produceScript;
                ch = {
                  description = "clickhouse client connected to the local ClickHouse";
                  text = ''exec ${chClient} "$@"'';
                };
                engine = {
                  description = "Run the chlive-engine CLI against the stack, e.g. `engine --explain \"select * from events\"`";
                  text = ''
                    cd "$DNVR_ROOT"
                    exec cargo run -q -p chlive-engine -- "$@"
                  '';
                };
                e2e = {
                  description = "Run the tests that need the running stack: the engine's, then the server end to end";
                  text = ''
                    cd "$DNVR_ROOT"
                    CHLIVE_E2E=1 exec cargo test -p chlive-engine -p chlive-e2e "$@"
                  '';
                };
                reset-data = {
                  description = "Delete local Redpanda and ClickHouse data (run while `dnvr up` is stopped)";
                  text = ''
                    # Kafka (with Connect's own topics) and ClickHouse (with the sink's KeeperMap
                    # offsets) must be reset together.
                    docker rm -f chlive-redpanda >/dev/null 2>&1 || true
                    docker volume rm chlive-redpanda >/dev/null 2>&1 || true
                    rm -rf "''${DNVR_ROOT:?}/${config.processes.clickhouse.dataDir}"
                    echo "removed Redpanda and ClickHouse data; next \`dnvr up\` starts empty and re-seeds"
                  '';
                };
              };
            };
        };
    };
}
