# The Rust toolchain: the dev shells' and chlive-wasm's. rustc as nixpkgs has it, plus the
# wasm32 target (chlive-wasm, the TS client's core).
{ pkgs }:
pkgs.rust-bin.stable."1.98.1".default.override {
  targets = [ "wasm32-unknown-unknown" ];
  extensions = [
    "rust-src"
    "rust-analyzer"
  ];
}
