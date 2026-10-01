# Commands on the dev shell's PATH (also `dnvr <name>`).
{ config, chlive, ... }:
{
  scripts = {
    ch = {
      description = "clickhouse client connected to main";
      text = ''exec ${chlive.chClient} "$@"'';
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
}
