# Command line for `clickhouse client` against main, as the default user. It brings its own
# config file, so ~/.clickhouse-client/config.xml (user, TLS, ...) never applies.
{ writeText, settings }:
let
  config = writeText "chlive-clickhouse-client.xml" ''
    <config>
      <host>127.0.0.1</host>
      <port>${toString settings.clickhouse.tcpPort}</port>
      <user>default</user>
      <password></password>
      <secure>false</secure>
    </config>
  '';
in
"clickhouse client --config-file ${config}"
