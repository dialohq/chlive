# chlive: the server (`chlive`, WebSocket over chlive-engine) and the terminal client (`clc`,
# chlive-tui). Links rdkafka dynamically and libchdb
# (./libchdb.nix), whose store path build.rs embeds as an rpath.
#
# Built with crane: the dependencies once (chlive-deps, rebuilt only when Cargo.lock's change),
# then the workspace's own crates on top of them.
{
  lib,
  craneLib,
  pkg-config,
  rustPlatform,
  rdkafka,
  zlib,
  openssl,
  libchdb,
}:
let
  workspace = import ./workspace.nix { inherit lib craneLib; };

  common = {
    inherit (workspace) cargoVendorDir;
    strictDeps = true;
    cargoExtraArgs = "--locked -p chlive-server -p chlive-tui";

    nativeBuildInputs = [
      pkg-config
      # chdb-rust's bindgen
      rustPlatform.bindgenHook
    ];
    buildInputs = [
      rdkafka
      zlib
      # chdb-rust's build script (reqwest with native-tls; it downloads nothing, CHDB_LIB_DIR is set)
      openssl
      libchdb
    ];

    env = {
      CHDB_LIB_DIR = "${libchdb}/lib";
      CHDB_INCLUDE_DIR = "${libchdb}/include";
    };

    # bindgenHook's clang args carry the cc wrapper's -frandom-seed (from $out), which differs
    # here and in the dependencies' build: cargo would rebuild bindgen and chdb-rust for it
    preBuild = ''
      export BINDGEN_EXTRA_CLANG_ARGS="$(sed 's/-frandom-seed=[^ ]*//' <<<"$BINDGEN_EXTRA_CLANG_ARGS")"
    '';
  };

  # chlive-deps-0: a fixed version, so a member's version bump doesn't rebuild them
  cargoArtifacts = craneLib.buildDepsOnly (
    common
    // {
      pname = "chlive";
      version = "0";
      inherit (workspace) dummySrc;
      # no `cargo check` pass: it only caches what clippy would need
      cargoCheckCommand = "true";
    }
  );
in
craneLib.buildPackage (
  common
  // {
    pname = "chlive";
    version = (lib.importTOML ../../server/Cargo.toml).package.version;
    inherit (workspace) src;
    inherit cargoArtifacts;

    # The server's tests run over a fake history and in-memory events (the engine's need the stack).
    doCheck = true;

    passthru.deps = cargoArtifacts;

    meta = {
      description = "Live ClickHouse queries over WebSocket (`chlive`), and a terminal client for them (`clc`)";
      license = lib.licenses.mit;
      mainProgram = "chlive";
    };
  }
)
