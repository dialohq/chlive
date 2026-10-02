{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    dnvr.url = "github:dialohq/dnvr";
    dnvr.inputs.nixpkgs.follows = "nixpkgs";
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
          # examples/simple/vite.config.js, which must agree with them.
          settings = {
            kafkaPort = 9092; # Redpanda Kafka API (its standard port), published to the host
            pandaproxyPort = 8082; # Redpanda HTTP proxy (its standard port; the example's writes)
            clickhouse = {
              # 18123/19000 rather than 8123/9000, so it can run next to another local ClickHouse
              httpPort = 18123;
              tcpPort = 19000; # native protocol (`ch`)
            };
            simplePort = 5174;
            topics = [
              "events"
              "customers"
              "orders"
            ];
            # events seeded into an empty ClickHouse on `dnvr up` (scripts/seed.sql)
            seedCount = 10;
          };

          clickhouse = pkgs.callPackage ./nix/packages/clickhouse.nix { };
          redpanda = pkgs.callPackage ./nix/packages/redpanda.nix {
            inherit dnvrState;
            name = "chlive-redpanda";
            inherit (settings) kafkaPort pandaproxyPort topics;
          };
          # `produce <topic> '<json>' …`, or JSON lines on stdin: one Kafka message per line.
          produceScript = {
            description = "Produce JSON events to a topic: `produce events '{\"user_id\": \"u1\"}'` (or lines on stdin)";
            text = ''
              topic="''${1:?usage: produce <topic> [json …] (or JSON lines on stdin)}"
              shift
              if [ "$#" -gt 0 ]; then
                printf '%s\n' "$@" | docker exec -i chlive-redpanda rpk topic produce "$topic"
              else
                docker exec -i chlive-redpanda rpk topic produce "$topic"
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
          };

        in
        {
          formatter = pkgs.nixfmt;
          packages = {
            inherit clickhouse redpanda;
          };

          dnvr.shells.default =
            { config, ... }:
            {
              description = "chlive: Redpanda (Docker) + ClickHouse + chlive-server (WebSocket over chlive-engine) + examples";

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
                # Writable storage for SQL-created users (chlive_ro, see scripts/schema.sql),
                # relative to DNVR_ROOT like the preset's other paths.
                clickhouse = {
                  imports = [ presets.clickhouse ];
                  package = clickhouse;
                  database = "default";
                  inherit (settings.clickhouse) httpPort tcpPort;
                  dataDir = ".dnvr/clickhouse";
                  timezone = "UTC";
                  extraConfigXml = "<access_control_path>.dnvr/clickhouse/access/</access_control_path>";
                };

                # Schema, Kafka-engine ingestion and the readonly user. Idempotent; reruns on
                # every start and then idles so its `ready` key stays live.
                schema = {
                  env = {
                    CH_DATABASE = "dnvr://clickhouse/database";
                    KAFKA_TOPICS = "dnvr://redpanda/topics";
                  };
                  command = ''
                    cd "$DNVR_ROOT"
                    ${chClient} --multiquery < scripts/schema.sql
                    dnvr-state set ready 1
                    echo "[schema] applied scripts/schema.sql"
                    exec tail -f /dev/null
                  '';
                };

                # Seed historical events (scripts/seed.sql, produced through Kafka) when ClickHouse has
                # fewer than $SEED_COUNT, then wait until main has ingested them.
                seed = {
                  env.SCHEMA_READY = "dnvr://schema/ready";
                  command = ''
                    cd "$DNVR_ROOT"
                    count() { ${chClient} --query "SELECT count() FROM default.events"; }
                    have=$(count)
                    if [ "$have" -lt "$SEED_COUNT" ]; then
                      echo "[seed] ClickHouse has $have events; seeding $((SEED_COUNT - have))"
                      ${chClient} --param_count=$((SEED_COUNT - have)) --queries-file scripts/seed.sql
                      until [ "$(count)" -ge "$SEED_COUNT" ]; do sleep 1; done
                      echo "[seed] ClickHouse now has $(count) events"
                    else
                      echo "[seed] ClickHouse already has $have events"
                    fi
                    dnvr-state set seeded 1
                    exec tail -f /dev/null
                  '';
                };

                # On its defaults: :8080, this ClickHouse (as chlive_ro) and this Redpanda.
                server = {
                  env.SCHEMA_READY = "dnvr://schema/ready";
                  command = ''
                    cd "$DNVR_ROOT"
                    exec cargo run --release -p chlive-server
                  '';
                };

                simple = {
                  command = ''
                    cd "$DNVR_ROOT"
                    [ -d node_modules ] || bun install
                    exec bun --bun run --cwd examples/simple dev --port ${toString settings.simplePort} --strictPort
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
                    # Kafka offsets and ClickHouse's _offset watermarks must be reset together.
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
