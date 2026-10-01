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
          lib,
          system,
          presets,
          dnvrState,
          ...
        }:
        let
          inherit (pkgs.stdenv.hostPlatform) isLinux;

          # chDB (embedded ClickHouse) is not in nixpkgs. Its engine version MUST match the
          # main ClickHouse below: aggregate states cross between them in binary form.
          libchdbVersion = "26.7.3"; # = ClickHouse 26.7.2.1
          libchdbAssets = {
            aarch64-darwin = {
              name = "macos-arm64";
              sha256 = "5640e50dccf711bf3dd5551333d08e43f433edf7bd94b2289f36c2539e627762";
            };
            x86_64-linux = {
              name = "linux-x86_64";
              sha256 = "bc33260c32acf78eade2ac41a9115f38e00404651fa42e3bb4c419e4f011c031";
            };
            aarch64-linux = {
              name = "linux-aarch64";
              sha256 = "d153adad1ff39b2e3caf0417f09d8bd9edd41939c7c67a3c4978a61e73fb9227";
            };
          };
          libchdb = pkgs.stdenv.mkDerivation {
            pname = "libchdb";
            version = libchdbVersion;
            src = pkgs.fetchurl {
              url = "https://github.com/chdb-io/chdb-core/releases/download/v${libchdbVersion}/${libchdbAssets.${system}.name}-libchdb.tar.gz";
              inherit (libchdbAssets.${system}) sha256;
            };
            sourceRoot = ".";
            nativeBuildInputs = lib.optionals isLinux [ pkgs.autoPatchelfHook ];
            buildInputs = lib.optionals isLinux [ pkgs.stdenv.cc.cc.lib ];
            dontConfigure = true;
            dontBuild = true;
            dontStrip = true;
            installPhase = ''
              install -Dm644 libchdb.so $out/lib/libchdb.so
              install -Dm644 chdb.h $out/include/chdb.h
            '';
          };

          # Main ClickHouse, pinned to chDB's engine version (nixpkgs has 26.5). Official
          # release binaries: a single `clickhouse` multi-tool.
          clickhouseVersion = "26.7.2.59";
          clickhouseAssets =
            let
              base = "https://github.com/ClickHouse/ClickHouse/releases/download/v${clickhouseVersion}-stable";
            in
            {
              aarch64-darwin = {
                url = "${base}/clickhouse-macos-aarch64";
                sha256 = "003gy3c5r4hprakgd071qgyir4z4jliwb7mdlir03yn5lkdasl2d";
              };
              x86_64-linux = {
                url = "${base}/clickhouse-common-static-${clickhouseVersion}-amd64.tgz";
                sha512 = "cafbb6a6651ee72210d2ee6bb67c1c2b4d2aee8cf6197df02c8323f674213c335e114acacc94e70190dfb77bbb5e82869f6e08c4eb3058f8972b12f86ed95a1a";
              };
              aarch64-linux = {
                url = "${base}/clickhouse-common-static-${clickhouseVersion}-arm64.tgz";
                sha512 = "6945f48dc72f539072d2e102abfb806dac2f65d80d7f78e4b646cd1aa451433fd84d91a4551fc26366fec9414f44a239d9e5fa40942868fc80e7be909f0a7a25";
              };
            };
          clickhouse = pkgs.stdenvNoCC.mkDerivation {
            pname = "clickhouse";
            version = clickhouseVersion;
            src = pkgs.fetchurl clickhouseAssets.${system};
            dontUnpack = true;
            dontStrip = true;
            installPhase = ''
              mkdir -p $out/bin
              ${
                if isLinux then
                  ''
                    tar xzf $src
                    install -m755 "$(find . -path '*/bin/clickhouse' -type f | head -1)" $out/bin/clickhouse
                  ''
                else
                  "install -m755 $src $out/bin/clickhouse"
              }
              # Release binaries are self-extracting: the first run decompresses the binary in
              # place, which must happen here while $out is still writable.
              HOME=$TMPDIR $out/bin/clickhouse --version
              for tool in server client local; do ln -s clickhouse $out/bin/clickhouse-$tool; done
            '';
          };

          kafkaPort = 19092;
          pandaproxyPort = 18082;
          topics = [
            "events"
            "customers"
            "orders"
          ];

          # Redpanda (Kafka API + HTTP proxy) in Docker; `docker` comes from the host. Data
          # persists in the `chlive-redpanda` volume, alongside main's data in .dnvr/clickhouse.
          redpandaCommand = pkgs.writeShellApplication {
            name = "chlive-redpanda";
            runtimeInputs = [
              pkgs.coreutils
              pkgs.gnugrep
              dnvrState
            ];
            text = ''
              name=chlive-redpanda
              docker rm -f "$name" >/dev/null 2>&1 || true
              # The HTTP proxy batches produce requests for 100 ms by default; disable that so a
              # single write from the simple demo is acknowledged (and visible) immediately.
              # The proxy's own Kafka client uses the in-container listener on 9092 (not
              # published); the host (main ClickHouse, server, generator) uses 19092.
              docker run --rm --name "$name" \
                -p 127.0.0.1:${toString kafkaPort}:${toString kafkaPort} \
                -p 127.0.0.1:${toString pandaproxyPort}:${toString pandaproxyPort} \
                -v chlive-redpanda:/var/lib/redpanda/data \
                --entrypoint /bin/bash \
                redpandadata/redpanda:v25.2.1 -c '
                  rpk redpanda config set pandaproxy_client.produce_batch_delay_ms 0 &&
                  exec /entrypoint.sh redpanda start --mode dev-container --smp 1 \
                    --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:${toString kafkaPort} \
                    --advertise-kafka-addr internal://127.0.0.1:9092,external://127.0.0.1:${toString kafkaPort} \
                    --pandaproxy-addr 0.0.0.0:${toString pandaproxyPort} \
                    --advertise-pandaproxy-addr 127.0.0.1:${toString pandaproxyPort}' &
              pid=$!
              trap 'docker stop -t 10 "$name" >/dev/null 2>&1 || true; wait $pid 2>/dev/null || true' EXIT INT TERM

              until docker exec "$name" rpk cluster health 2>/dev/null | grep -qE 'Healthy:.+true'; do
                kill -0 $pid 2>/dev/null || { echo "[redpanda] exited before becoming ready" >&2; exit 1; }
                sleep 1
              done
              for t in ${lib.concatStringsSep " " topics}; do
                docker exec "$name" rpk topic create "$t" -p 3 -r 1 >/dev/null 2>&1 || true
              done
              dnvr-state set bootstrap 127.0.0.1:${toString kafkaPort}
              # readiness key: topics exist
              dnvr-state set topics ${lib.concatStringsSep "," topics}
              echo "[redpanda] ready: kafka 127.0.0.1:${toString kafkaPort}, http proxy 127.0.0.1:${toString pandaproxyPort}"
              wait $pid
            '';
          };

          # Main ClickHouse is reached as the default user on these fixed ports (see chlive.toml).
          # Own client config, so ~/.clickhouse-client/config.xml (user, TLS, ...) never applies.
          chClientConfig = pkgs.writeText "chlive-clickhouse-client.xml" ''
            <config>
              <host>127.0.0.1</host>
              <port>19000</port>
              <user>default</user>
              <password></password>
              <secure>false</secure>
            </config>
          '';
          chClient = "clickhouse client --config-file ${chClientConfig}";
        in
        {
          formatter = pkgs.nixfmt;
          packages = { inherit libchdb clickhouse; };

          dnvr.shells.default =
            { config, ... }:
            {
              description = "chlive: Redpanda (Docker) + ClickHouse main + chlive-server (embedded chDB) + demos";

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

                # client / demos
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
                CHLIVE_KAFKA = "127.0.0.1:${toString kafkaPort}";
                # events seeded into an empty main on `dnvr up`; default rate of the `stream` script
                SEED_COUNT = "1000000";
                STREAM_RATE = "200";
              };

              processes = {
                redpanda.command = redpandaCommand;

                # Main ClickHouse via dnvr's preset, on the version chDB was built from.
                clickhouse = {
                  imports = [ presets.clickhouse ];
                  package = clickhouse;
                  database = "default";
                  # 18123/19000 rather than 8123/9000, so it can run next to another local ClickHouse
                  httpPort = 18123;
                  # native protocol, used by remote() from the chDB cache inside chlive-server
                  tcpPort = 19000;
                  timezone = "UTC";
                  # Writable storage for SQL-created users (chlive_ro, see infra/main-init.sql);
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
                    ${chClient} --multiquery < infra/main-init.sql
                    dnvr-state set ready 1
                    echo "[schema] applied infra/main-init.sql"
                    exec tail -f /dev/null
                  '';
                };

                # Seed historical events through Kafka when main has fewer than $SEED_COUNT.
                seed = {
                  env.SCHEMA_READY = "dnvr://schema/ready";
                  command = ''
                    cd "$DNVR_ROOT"
                    have=$(${chClient} --query "SELECT count() FROM default.events")
                    if [ "$have" -lt "$SEED_COUNT" ]; then
                      echo "[seed] main has $have events; seeding $((SEED_COUNT - have))"
                      cargo run --release -p chlive-generator -- seed --count $((SEED_COUNT - have))
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

                demo = {
                  command = ''
                    cd "$DNVR_ROOT"
                    [ -d node_modules ] || npm install
                    dnvr-state set deps 1
                    exec npm run dev -w demo -- --port 5173 --strictPort
                  '';
                };

                simple = {
                  env.NPM_DEPS = "dnvr://demo/deps";
                  command = ''
                    cd "$DNVR_ROOT"
                    exec npm run dev -w simple -- --port 5174 --strictPort
                  '';
                };
              };

              scripts = {
                ch = {
                  description = "clickhouse client connected to main";
                  text = ''exec ${chClient} "$@"'';
                };
                e2e = {
                  description = "Run the end-to-end tests (no `stream` running)";
                  text = ''
                    cd "$DNVR_ROOT"
                    CHLIVE_E2E=1 exec cargo test -p chlive-e2e "$@"
                  '';
                };
                # Live traffic is opt-in: run it in a terminal while the stack is up.
                stream = {
                  description = "Produce live events: `stream [rate]`, default 200/s, until Ctrl-C";
                  text = ''
                    cd "$DNVR_ROOT"
                    exec cargo run --release -p chlive-generator -- stream --rate "''${1:-$STREAM_RATE}"
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
