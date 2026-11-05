# chlive-wasm: chlive-core for the TS client, as WebAssembly. The `wasm` script's client/wasm/
# directory (wasm-bindgen's `--target web` JS and the wasm-opt'd module), for other flakes to
# put in client/wasm/ when they build @chlive/client.
{
  lib,
  makeRustPlatform,
  rust,
  wasm-bindgen-cli,
  binaryen,
}:
let
  rustPlatform = makeRustPlatform {
    cargo = rust;
    rustc = rust;
  };
in
rustPlatform.buildRustPackage {
  pname = "chlive-wasm";
  version = (lib.importTOML ../../wasm/Cargo.toml).package.version;

  # The whole workspace: cargo resolves every member, even when building one.
  src = lib.fileset.toSource {
    root = ../..;
    fileset = lib.fileset.unions [
      ../../Cargo.toml
      ../../Cargo.lock
      ../../server
      ../../core
      ../../engine
      ../../wasm
      ../../protocol
      ../../client-rs
      ../../tests
      ../../examples/rust-alerts
      ../../tui
    ];
  };
  cargoLock.lockFile = ../../Cargo.lock;

  # wasm-bindgen's CLI must be the `wasm-bindgen` crate's version (wasm/Cargo.toml)
  nativeBuildInputs = [
    wasm-bindgen-cli
    binaryen
  ];

  # as the `wasm` script (nix/shell.nix) builds it
  buildPhase = ''
    runHook preBuild
    cargo build -p chlive-wasm --target wasm32-unknown-unknown --profile wasm --offline
    wasm-bindgen --target web --out-dir $out target/wasm32-unknown-unknown/wasm/chlive_wasm.wasm
    wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
      -o $out/chlive_wasm_bg.wasm $out/chlive_wasm_bg.wasm
    runHook postBuild
  '';
  installPhase = "true";
  doCheck = false;

  meta = {
    description = "chlive-core for the TS client, as WebAssembly (@chlive/client's wasm/ directory)";
    license = lib.licenses.mit;
  };
}
