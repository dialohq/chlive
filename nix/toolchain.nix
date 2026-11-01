# The dev shells' toolchain: what's on PATH, and the env the Rust build needs.
{
  pkgs,
  dnvrState,
  libchdb,
}:
let
  # rustc as nixpkgs has it, plus the wasm32 target (chlive-wasm, the TS client's core)
  rust = pkgs.rust-bin.stable."1.98.1".default.override {
    targets = [ "wasm32-unknown-unknown" ];
    extensions = [
      "rust-src"
      "rust-analyzer"
    ];
  };
in
{
  packages = with pkgs; [
    nixfmt

    # Rust toolchain (cargo, rustc, clippy, rustfmt, rust-analyzer), and for the WebAssembly
    # build: wasm-bindgen's CLI (the `wasm-bindgen` crate is pinned to its version) and wasm-opt
    rust
    wasm-bindgen-cli
    binaryen

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
    RUST_SRC_PATH = "${rust}/lib/rustlib/src/rust/library";
    # chdb-rust links against these instead of downloading an engine at build time;
    # build.rs files embed CHDB_LIB_DIR as an rpath.
    CHDB_LIB_DIR = "${libchdb}/lib";
    CHDB_INCLUDE_DIR = "${libchdb}/include";
    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
  };
}
