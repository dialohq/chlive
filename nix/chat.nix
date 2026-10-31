# The chat demo's dnvr shell (`nix develop .#chat`, then `dnvr up`): its own stack
# (nix/stack.nix, every name prefixed `chat-`), its schema and seed, the app, and scripts.
# Runs instead of the main stack, not next to it.
{
  pkgs,
  presets,
  dnvrState,
  continuous-backup,
  clickhouse,
  libchdb,
}:
let
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
        prefix = "chat-";
        topics = [
          "messages"
          "users"
        ];
        schemaFile = "scripts/chat/schema.sql";
        # permissions from WorkOS, or Postgres with CHAT_SECURITY=postgres (a shell expression)
        serverConfig = ''"nix/chlive.chat''${CHAT_SECURITY:+.$CHAT_SECURITY}.toml"'';
        cacheDir = ".dnvr/chat-chlive-cache";
      };
  inherit (stack) chClient name ref;

  toolchain = import ./toolchain.nix { inherit pkgs dnvrState libchdb; };

  # The permissions source's Postgres: a fixed port (dnvr's preset doesn't pick one), and the
  # `chat` database.
  pgPort = 54329;
  psql = ''${pkgs.postgresql}/bin/psql -h 127.0.0.1 -p ${toString pgPort} -U postgres -d chat'';
in
{ ... }:
{
  description = "chlive chat demo: its own Redpanda, ClickHouse, sink, pause server and chlive-server, Postgres (a permissions source; WorkOS is the default), and the chat app";

  packages = toolchain.packages ++ [
    clickhouse
    pkgs.postgresql
  ];
  inherit (toolchain) env;
  inherit (stack) prerun;
  # the stack runs as a Rex session, a tab per process (`dnvr up --runner tmux` for tmux)
  defaultRunner = "rex";

  processes = stack.processes // {
    # with CHAT_SECURITY=postgres the server reads its permissions from Postgres: once its
    # schema is in (WorkOS: WORKOS_API_KEY, from the shell)
    ${name "server"} = stack.processes.${name "server"} // {
      env = stack.processes.${name "server"}.env // {
        PG_READY = ref "postgres-schema" "ready";
      };
    };

    # Postgres, where the permissions live with CHAT_SECURITY=postgres (the source chlive
    # follows with logical replication, CDC). dnvr's preset: data under .dnvr/chat-postgres,
    # `database` published once the database exists.
    ${name "postgres"} = {
      imports = [ presets.postgres ];
      package = pkgs.postgresql;
      database = "chat";
      port = pgPort;
      settings = {
        wal_level = "logical";
        max_replication_slots = 10;
        max_wal_senders = 10;
      };
    };

    # The permissions table and its publication (scripts/chat/postgres.sql). Idempotent; reruns
    # on every start and then idles so its `ready` key stays live.
    ${name "postgres-schema"} = {
      env.PG_DATABASE = ref "postgres" "database";
      command = ''
        cd "$DNVR_ROOT"
        ${psql} -v ON_ERROR_STOP=1 -q -f scripts/chat/postgres.sql
        dnvr-state set ready 1
        echo "[postgres-schema] applied scripts/chat/postgres.sql (wal_level: $(${psql} -tAc 'show wal_level'))"
        exec tail -f /dev/null
      '';
    };

    # Users and an hour of messages (scripts/chat/seed), produced to
    # Kafka like everything else, into an empty ClickHouse; then wait until they're in.
    ${name "seed"} = {
      env = {
        CONNECTORS_READY = ref "connectors" "ready";
        CH_TCP_PORT = ref "clickhouse" "tcpPort";
        PROXY_URL = ref "redpanda" "proxyUrl";
      };
      command = ''
        cd "$DNVR_ROOT/scripts/chat"
        count() { ${chClient ''"$CH_TCP_PORT"''} --query "SELECT count() FROM default.$1"; }
        if [ "$(count users)" -eq 0 ]; then
          ${stack.produce} users < seed/users.jsonl >/dev/null
          clickhouse local --queries-file seed/messages.sql | ${stack.produce} messages >/dev/null
          until [ "$(count users)" -ge 4 ] && [ "$(count messages)" -ge 60 ]; do sleep 1; done
          echo "[seed] seeded $(count users) users, $(count messages) messages"
        else
          echo "[seed] already seeded ($(count messages) messages)"
        fi
        dnvr-state set seeded 1
        exec tail -f /dev/null
      '';
    };

    # The app (Vite): /live proxied to the server, /kafka to Redpanda's HTTP proxy.
    ${name "app"} = {
      env = {
        CHLIVE_URL = ref "server" "url";
        PROXY_URL = ref "redpanda" "proxyUrl";
      };
      command = ''
        cd "$DNVR_ROOT"
        [ -d node_modules/react ] || bun install
        port=$(dnvr-state pick-port)
        dnvr-state set url "http://localhost:$port"
        exec bun --bun run --cwd examples/chat dev --port "$port" --strictPort
      '';
    };
  };

  scripts = stack.scripts // {
    pg = {
      description = "psql connected to the chat stack's Postgres (the `chat` database)";
      text = ''exec ${psql} "$@"'';
    };
    reset-data = {
      inherit (stack.scripts.reset-data) description;
      text = ''
        ${stack.scripts.reset-data.text}
        rm -rf "''${DNVR_ROOT:?}/.dnvr/${name "postgres"}"
        echo "removed Postgres data too"
      '';
    };
    workos-setup = {
      description = "Set up the chat's permissions in WorkOS (WORKOS_API_KEY): users, roles, feeds; adds only what's missing";
      text = ''
        cd "$DNVR_ROOT"
        exec bun scripts/chat/workos.ts "$@"
      '';
    };
    chat-smoke = {
      description = "Check the running chat stack end to end: messages through chlive, per user's WorkOS (or, with CHAT_SECURITY=postgres, Postgres) permissions";
      runtimeInputs = [
        dnvrState
        pkgs.postgresql
      ];
      text = ''
        CHLIVE_URL="''${CHLIVE_URL:-$(dnvr-state get ${name "server"}.url)}"
        PROXY_URL="''${PROXY_URL:-$(dnvr-state get ${name "redpanda"}.proxyUrl)}"
        PG_URL="''${PG_URL:-postgres://postgres@127.0.0.1:${toString pgPort}/chat}"
        export CHLIVE_URL PROXY_URL PG_URL
        cd "$DNVR_ROOT"
        if [ "''${CHAT_SECURITY:-}" = postgres ]; then
          exec bun scripts/chat/smoke.ts "$@"
        fi
        exec bun scripts/chat/smoke-workos.ts "$@"
      '';
    };
  };
}
