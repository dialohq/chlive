# A local chlive stack, for a dnvr shell: Redpanda (Docker), ClickHouse (with an embedded
# Keeper), Kafka Connect with one ClickHouse sink connector per topic (exactly once), the pause
# server, the schema and chlive-server. Shells add their own seed, apps and scripts.
#
# Every dnvr shell of the repo shares `.dnvr/`, where runtime keys are namespaced by process
# name: a stack's `prefix` goes in front of every process name, Docker name, path and Connect
# name, so two stacks don't step on each other (`""` for the main one, e.g. `"chat-"` for a
# demo's). Run one at a time.
#
# Every process picks its own ports (`dnvr-state pick-port`) and publishes its address; the
# others get it through `dnvr://<process>/<key>` env refs (see `ref`).
{
  pkgs,
  presets,
  dnvrState,
  continuous-backup,
  clickhouse,
}:
{
  prefix ? "",
  topics,
  # partitions per topic
  partitions ? 3,
  # applied on every start, relative to DNVR_ROOT
  schemaFile,
  # chlive-server's config file, relative to DNVR_ROOT (addresses come as flags)
  serverConfig,
  # chlive-server's result store (as in its config), for `reset-data`
  cacheDir,
}:
let
  # A process's name, and a ref to one of its keys.
  name = n: "${prefix}${n}";
  ref = n: key: "dnvr://${name n}/${key}";
  # Where a process keeps its files, relative to DNVR_ROOT.
  dir = n: ".dnvr/${name n}";

  redpanda = pkgs.callPackage ./packages/redpanda.nix {
    inherit dnvrState topics partitions;
    name = "chlive-${name "redpanda"}";
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
    name = "chlive-${prefix}sink-${topic}";
    stateTable = "chlive_sink_${topic}_state";
    keeperPath = "/chlive-sink/${topic}";
  };
  # ClickHouse's `hostname` and `port` are added at runtime, once it has published them.
  connectorConfig =
    topic:
    let
      c = sinkConnector topic;
    in
    pkgs.writeText "${c.name}.json" (
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
  connectProperties = pkgs.writeText "chlive-${prefix}connect.properties" ''
    group.id=chlive-${prefix}connect
    config.storage.topic=chlive-${prefix}connect-configs
    offset.storage.topic=chlive-${prefix}connect-offsets
    status.storage.topic=chlive-${prefix}connect-status
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
  # `listen`, `connectUrl`, `clickhouseUrl` and `keeperAddress` (ClickHouse's embedded Keeper,
  # where it elects itself leader and keeps its tokens) are added at runtime.
  pauseServerConfig = pkgs.writeText "chlive-${prefix}pause-server.json" (
    builtins.toJSON {
      keeperPath = "/chlive-pause-server";
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
  # with a plain POST to Redpanda's HTTP proxy (the same endpoint the apps use), at
  # $PROXY_URL or else the running stack's.
  produceScript = {
    description = "Produce JSON messages to a topic: `produce <topic> '{\"id\": \"x\"}'` (or JSON lines on stdin)";
    runtimeInputs = [ dnvrState ];
    text = ''
      topic="''${1:?usage: produce <topic> [json …] (or JSON lines on stdin)}"
      shift
      proxy="''${PROXY_URL:-$(dnvr-state get ${name "redpanda"}.proxyUrl)}"
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
  # `produce` as an executable, for processes (seeds).
  produce = pkgs.lib.getExe (
    pkgs.writeShellApplication {
      name = "produce";
      inherit (produceScript) text runtimeInputs;
    }
  );

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
    export CHLIVE_CLICKHOUSE_URL="''${CHLIVE_CLICKHOUSE_URL:-$(dnvr-state get ${name "clickhouse"}.httpUrl)}"
    export CHLIVE_KAFKA="''${CHLIVE_KAFKA:-$(dnvr-state get ${name "redpanda"}.bootstrap)}"
    export CHLIVE_PAUSE_SERVER="''${CHLIVE_PAUSE_SERVER:-$(dnvr-state get ${name "pause-server"}.url)}"
  '';
in
{
  inherit
    name
    ref
    produce
    chClient
    stackEnv
    ;

  # The ports of ClickHouse's embedded Keeper: ClickHouse's config reads them with `from_env`,
  # and the pause server connects to it.
  prerun = ''
    CHLIVE_KEEPER_PORT=$(dnvr-state pick-port)
    CHLIVE_KEEPER_RAFT_PORT=$(dnvr-state pick-port)
    export CHLIVE_KEEPER_PORT CHLIVE_KEEPER_RAFT_PORT
  '';

  # Start-up order follows the `dnvr://` refs in each process's `env`.
  processes = {
    # Publishes `bootstrap`, `proxyUrl` and `topics` once the topics exist.
    ${name "redpanda"}.command = redpanda;

    # ClickHouse via dnvr's preset, on our pinned version (nix/packages/clickhouse.nix); it picks
    # its HTTP and native ports and publishes `host`, `httpPort`, `tcpPort` and `httpUrl`, then
    # `database` once it answers queries. Writable storage for SQL-created users (see the
    # schema), relative to DNVR_ROOT like the preset's other paths; and an embedded Keeper, for
    # the sink's KeeperMap state tables.
    ${name "clickhouse"} = {
      imports = [ presets.clickhouse ];
      package = clickhouse;
      database = "default";
      httpPort = null;
      tcpPort = null;
      dataDir = dir "clickhouse";
      timezone = "UTC";
      extraConfigXml = ''
        <access_control_path>${dir "clickhouse"}/access/</access_control_path>
        <keeper_server>
          <tcp_port from_env="CHLIVE_KEEPER_PORT"/>
          <server_id>1</server_id>
          <log_storage_path>${dir "clickhouse"}/coordination/log</log_storage_path>
          <snapshot_storage_path>${dir "clickhouse"}/coordination/snapshots</snapshot_storage_path>
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
    ${name "schema"} = {
      env = {
        CH_DATABASE = ref "clickhouse" "database";
        CH_TCP_PORT = ref "clickhouse" "tcpPort";
      };
      command = ''
        cd "$DNVR_ROOT"
        ${chClient ''"$CH_TCP_PORT"''} --multiquery < ${schemaFile}
        dnvr-state set ready 1
        echo "[schema] applied ${schemaFile}"
        exec tail -f /dev/null
      '';
    };

    # Kafka Connect, running the ClickHouse sink connectors.
    ${name "connect"} = {
      env.KAFKA = ref "redpanda" "bootstrap";
      command = ''
        cd "$DNVR_ROOT"
        mkdir -p ${dir "connect"}
        port=$(dnvr-state pick-port)
        props=${dir "connect"}/connect.properties
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
    ${name "connectors"} = {
      env = {
        CONNECT_URL = ref "connect" "url";
        SCHEMA_READY = ref "schema" "ready";
        TOPICS = ref "redpanda" "topics";
        CH_HOST = ref "clickhouse" "host";
        CH_HTTP_PORT = ref "clickhouse" "httpPort";
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
    ${name "pause-server"} = {
      env = {
        CONNECTORS_READY = ref "connectors" "ready";
        CONNECT_URL = ref "connect" "url";
        CLICKHOUSE_URL = ref "clickhouse" "httpUrl";
      };
      command = ''
        cd "$DNVR_ROOT"
        mkdir -p ${dir "pause-server"}
        port=$(dnvr-state pick-port)
        conf=${dir "pause-server"}/config.json
        ${pkgs.jq}/bin/jq --arg listen "127.0.0.1:$port" \
          --arg connect "$CONNECT_URL" --arg clickhouse "$CLICKHOUSE_URL" \
          --arg keeper "127.0.0.1:''${CHLIVE_KEEPER_PORT:?}" \
          '. + {listen: $listen, connectUrl: $connect, clickhouseUrl: $clickhouse, keeperAddress: $keeper}' \
          ${pauseServerConfig} > "$conf"
        export CLICKHOUSE_USERNAME=default CLICKHOUSE_PASSWORD=
        ${supervise {
          name = "Pause server";
          start = ''${backup}/bin/durable-clickhouse-backup pause-server "$conf"'';
          # leading, not just up: a standby answers /health too, but refuses the rest with 503,
          # where the leader gives an unknown token 404
          readyWhen = ''[ "$(${pkgs.curl}/bin/curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d '{"token":"00000000-0000-0000-0000-000000000000"}' "http://127.0.0.1:$port/renew")" = 404 ]'';
          onReady = ''
            dnvr-state set url "http://127.0.0.1:$port"
            echo "[pause-server] ready"
          '';
        }}
      '';
    };

    # As chlive_ro (snapshots as chlive_snap), against this stack. Publishes `url` (the
    # WebSocket) and `httpUrl` (/healthz, /debug, /debug/state, /debug/subscriptions) as soon
    # as its port is picked: a cold `cargo build` can outlast the consumers' wait for them, and
    # both reconnect. The rest of its config: `serverConfig`.
    ${name "server"} = {
      env = {
        PAUSE_SERVER_URL = ref "pause-server" "url";
        CH_HTTP_URL = ref "clickhouse" "httpUrl";
        KAFKA_BROKERS = ref "redpanda" "bootstrap";
      };
      command = ''
        cd "$DNVR_ROOT"
        port=$(dnvr-state pick-port)
        dnvr-state set url "ws://127.0.0.1:$port/live"
        dnvr-state set httpUrl "http://127.0.0.1:$port"
        exec cargo run --release -p chlive-server -- --config ${serverConfig} \
          --listen "127.0.0.1:$port" --clickhouse-url "$CH_HTTP_URL" \
          --kafka "$KAFKA_BROKERS" --pause-server "$PAUSE_SERVER_URL"
      '';
    };
  };

  scripts = {
    produce = produceScript;
    ch = {
      description = "clickhouse client connected to the local ClickHouse";
      runtimeInputs = [ dnvrState ];
      text = ''exec ${chClient ''"$(dnvr-state get ${name "clickhouse"}.tcpPort)"''} "$@"'';
    };
    reset-data = {
      description = "Delete local Redpanda, ClickHouse and chlive cache data (run while `dnvr up` is stopped)";
      text = ''
        # Kafka (with Connect's own topics) and ClickHouse (with the sink's KeeperMap
        # offsets) must be reset together; chlive's stored results hold cursors into them.
        docker rm -f chlive-${name "redpanda"} >/dev/null 2>&1 || true
        docker volume rm chlive-${name "redpanda"} >/dev/null 2>&1 || true
        rm -rf "''${DNVR_ROOT:?}/${dir "clickhouse"}"
        rm -rf "''${DNVR_ROOT:?}/${cacheDir}"
        echo "removed Redpanda, ClickHouse and chlive cache data; next \`dnvr up\` starts empty and re-seeds"
      '';
    };
  };
}
