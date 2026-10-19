# The dev shells' toolchain: what's on PATH, and the env the Rust build needs.
{
  pkgs,
  dnvrState,
  libchdb,
}:
{
  packages = with pkgs; [
    nixfmt

    # Rust toolchain
    cargo
    rustc
    clippy
    rustfmt
    rust-analyzer

    # chdb-rust links libchdb; its bindgen needs libclang
    libchdb
    llvmPackages.libclang

    # native deps for the rdkafka crate (dynamic-linking feature)
    pkg-config
    rdkafka
    zlib

    # client / examples (Bun)
    bun

    jq
    curl
    dnvrState
  ];
  env = {
    RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
    # chdb-rust links against these instead of downloading an engine at build time;
    # build.rs files embed CHDB_LIB_DIR as an rpath.
    CHDB_LIB_DIR = "${libchdb}/lib";
    CHDB_INCLUDE_DIR = "${libchdb}/include";
    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
  };
}
