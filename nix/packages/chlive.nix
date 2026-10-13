# chlive: the server (`chlive`, WebSocket over chlive-engine) and the terminal client (`clc`,
# chlive-tui). Links rdkafka dynamically and libchdb
# (./libchdb.nix), whose store path build.rs embeds as an rpath.
{
  lib,
  rustPlatform,
  pkg-config,
  rdkafka,
  zlib,
  libchdb,
}:
let
  root = ../..;
in
rustPlatform.buildRustPackage {
  pname = "chlive";
  version = (lib.importTOML ../../server/Cargo.toml).package.version;

  # The whole workspace: cargo resolves every member, even when building one.
  src = lib.fileset.toSource {
    inherit root;
    fileset = lib.fileset.unions [
      ../../Cargo.toml
      ../../Cargo.lock
      ../../server
      ../../engine
      ../../protocol
      ../../client-rs
      ../../tests
      ../../examples/rust-alerts
      ../../tui
    ];
  };
  cargoLock.lockFile = ../../Cargo.lock;

  cargoBuildFlags = [
    "-p"
    "chlive-server"
    "-p"
    "chlive-tui"
  ];
  # The server's tests run over a fake history and in-memory events (the engine's need the stack).
  cargoTestFlags = [
    "-p"
    "chlive-server"
    "-p"
    "chlive-tui"
  ];

  nativeBuildInputs = [
    pkg-config
    # chdb-rust's bindgen
    rustPlatform.bindgenHook
  ];
  buildInputs = [
    rdkafka
    zlib
    libchdb
  ];

  env = {
    CHDB_LIB_DIR = "${libchdb}/lib";
    CHDB_INCLUDE_DIR = "${libchdb}/include";
  };

  meta = {
    description = "Live ClickHouse queries over WebSocket (`chlive`), and a terminal client for them (`clc`)";
    license = lib.licenses.mit;
    mainProgram = "chlive";
  };
}
