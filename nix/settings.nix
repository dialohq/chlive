# Ports and names shared by the dnvr processes. chlive.toml (server and e2e tests) and
# examples/simple/vite.config.js must agree with these.
{
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
}
