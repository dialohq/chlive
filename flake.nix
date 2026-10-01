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
          # Ports and names shared by the dnvr processes below. chlive.toml (server and e2e
          # tests) and examples/simple/vite.config.js must agree with these.
          settings = {
            kafkaPort = 19092; # Redpanda Kafka API, published to the host
            pandaproxyPort = 18082; # Redpanda HTTP proxy (the simple example's writes)
            clickhouse = {
              # 18123/19000 rather than 8123/9000, so it can run next to another local ClickHouse
              httpPort = 18123;
              # native protocol, used by remote() from the chDB cache inside chlive-server
              tcpPort = 19000;
            };
            simplePort = 5174;
            topics = [
              "events"
              "customers"
              "orders"
            ];
            # events seeded into an empty main on `dnvr up` (scripts/seed.sql)
            seedCount = 1000000;
          };

          libchdb = pkgs.callPackage ./nix/packages/libchdb.nix { };
          clickhouse = pkgs.callPackage ./nix/packages/clickhouse.nix { };
          redpanda = pkgs.callPackage ./nix/packages/redpanda.nix { inherit dnvrState settings; };

          # `clickhouse client` against main, as the default user. It brings its own config
          # file, so ~/.clickhouse-client/config.xml (user, TLS, ...) never applies.
          chClientConfig = pkgs.writeText "chlive-clickhouse-client.xml" ''
            <config>
              <host>127.0.0.1</host>
              <port>${toString settings.clickhouse.tcpPort}</port>
              <user>default</user>
              <password></password>
              <secure>false</secure>
            </config>
          '';
          chClient = "clickhouse client --config-file ${chClientConfig}";
        in
        {
          formatter = pkgs.nixfmt;
          packages = { inherit libchdb clickhouse redpanda; };

          dnvr.shells.default =
            { config, ... }:
            {
              description = "chlive: Redpanda (Docker) + ClickHouse main + chlive-server (embedded chDB) + examples";

              packages = with pkgs; [
                nixfmt

                # Rust toolchain
                cargo
                rustc
                clippy
                rustfmt
                rust-analyzer

                # embedded cache: chdb-rust links libchdb; its bindgen needs libclang
                libchdb
                llvmPackages.libclang

                # native deps for the rdkafka crate (dynamic-linking feature)
                pkg-config
                rdkafka
                zlib

                # client / examples
                nodejs_24

                # main ClickHouse (also on PATH for ad-hoc use; Redpanda runs in Docker)
                clickhouse

                jq
                curl
              ];

              env = {
                RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
                # chdb-rust links against these instead of downloading an engine at build time.
                CHDB_LIB_DIR = "${libchdb}/lib";
                CHDB_INCLUDE_DIR = "${libchdb}/include";
                LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
                SEED_COUNT = toString settings.seedCount;
              };

              # Start-up order follows the `dnvr://` refs in each process's `env`.
              processes = {
                redpanda.command = redpanda;

                # Main ClickHouse via dnvr's preset, on the version chDB was built from.
                clickhouse = {
                  imports = [ presets.clickhouse ];
                  package = clickhouse;
                  database = "default";
                  inherit (settings.clickhouse) httpPort tcpPort;
                  timezone = "UTC";
                  # Writable storage for SQL-created users (chlive_ro, see scripts/schema.sql);
                  # relative to DNVR_ROOT like the preset's other paths.
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

                # Seed historical events (scripts/seed.sql, produced through Kafka) when main has
                # fewer than $SEED_COUNT, then wait until main has ingested them.
                seed = {
                  env.SCHEMA_READY = "dnvr://schema/ready";
                  command = ''
                    cd "$DNVR_ROOT"
                    count() { ${chClient} --query "SELECT count() FROM default.events"; }
                    have=$(count)
                    if [ "$have" -lt "$SEED_COUNT" ]; then
                      echo "[seed] main has $have events; seeding $((SEED_COUNT - have))"
                      ${chClient} --param_count=$((SEED_COUNT - have)) --queries-file scripts/seed.sql
                      until [ "$(count)" -ge "$SEED_COUNT" ]; do sleep 1; done
                      echo "[seed] main now has $(count) events"
                    else
                      echo "[seed] main already has $have events"
                    fi
                    dnvr-state set seeded 1
                    exec tail -f /dev/null
                  '';
                };

                server = {
                  env.SCHEMA_READY = "dnvr://schema/ready";
                  command = ''
                    cd "$DNVR_ROOT"
                    exec cargo run --release -p chlive-server -- --config chlive.toml
                  '';
                };

                simple = {
                  command = ''
                    cd "$DNVR_ROOT"
                    [ -d node_modules ] || npm install
                    exec npm run dev -w examples/simple -- --port ${toString settings.simplePort} --strictPort
                  '';
                };
              };

              scripts = {
                ch = {
                  description = "clickhouse client connected to main";
                  text = ''exec ${chClient} "$@"'';
                };
                e2e = {
                  description = "Run the end-to-end tests against the running stack";
                  text = ''
                    cd "$DNVR_ROOT"
                    CHLIVE_E2E=1 exec cargo test -p chlive-e2e "$@"
                  '';
                };
                reset-data = {
                  description = "Delete local Redpanda and ClickHouse data (run while `dnvr up` is stopped)";
                  text = ''
                    # Kafka offsets and main's _offset watermarks must be reset together.
                    docker rm -f chlive-redpanda >/dev/null 2>&1 || true
                    docker volume rm chlive-redpanda >/dev/null 2>&1 || true
                    rm -rf "$DNVR_ROOT/${config.processes.clickhouse.dataDir}"
                    echo "removed Redpanda and ClickHouse data; next \`dnvr up\` starts empty and re-seeds"
                  '';
                };
              };
            };
        };
    };
}
