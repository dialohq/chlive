# Processes `dnvr up` runs. Start-up order follows the `dnvr://` refs in `env`.
{ presets, chlive, ... }:
let
  inherit (chlive) chClient settings;
in
{
  processes = {
    redpanda.command = chlive.redpanda;

    # Main ClickHouse via dnvr's preset, on the version chDB was built from.
    clickhouse = {
      imports = [ presets.clickhouse ];
      package = chlive.clickhouse;
      database = "default";
      inherit (settings.clickhouse) httpPort tcpPort;
      timezone = "UTC";
      # Writable storage for SQL-created users (chlive_ro, see scripts/schema.sql);
      # relative to DNVR_ROOT like the preset's other paths.
      extraConfigXml = "<access_control_path>.dnvr/clickhouse/access/</access_control_path>";
    };

    # Schema, Kafka-engine ingestion and the readonly user. Idempotent; reruns on every start
    # and then idles so its `ready` key stays live.
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

    # Seed historical events (scripts/seed.sql, produced through Kafka) when main has fewer
    # than $SEED_COUNT, then wait until main has ingested them.
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
}
