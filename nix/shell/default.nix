# The dnvr dev shell: toolchain and env here, processes and scripts in their own modules.
# `chlive` comes from `dnvr.specialArgs` in flake.nix.
{ pkgs, chlive, ... }:
{
  imports = [
    ./processes.nix
    ./scripts.nix
  ];

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
    chlive.libchdb
    llvmPackages.libclang

    # native deps for the rdkafka crate (dynamic-linking feature)
    pkg-config
    rdkafka
    zlib

    # client / examples
    nodejs_24

    # main ClickHouse (also on PATH for ad-hoc use; Redpanda runs in Docker)
    chlive.clickhouse

    jq
    curl
  ];

  env = {
    RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
    # chdb-rust links against these instead of downloading an engine at build time.
    CHDB_LIB_DIR = "${chlive.libchdb}/lib";
    CHDB_INCLUDE_DIR = "${chlive.libchdb}/include";
    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
    SEED_COUNT = toString chlive.settings.seedCount;
  };
}
