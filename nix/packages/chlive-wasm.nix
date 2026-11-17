# chlive-wasm: chlive-core for the TS client, as WebAssembly. The `wasm` script's client/wasm/
# directory (wasm-bindgen's `--target web` JS and the wasm-opt'd module), for other flakes to
# put in client/wasm/ when they build @chlive/client.
#
# Built with crane, on nix/rust.nix's toolchain (nixpkgs' rustc has no wasm32 target): the
# dependencies once (chlive-wasm-deps, rebuilt only when Cargo.lock's change), then the crate.
{
  lib,
  craneLib,
  # ./wasm-bindgen-cli.nix: the `wasm-bindgen` crate's version
  wasm-bindgen-cli,
  binaryen,
  removeReferencesTo,
}:
let
  workspace = import ./workspace.nix { inherit lib craneLib; };

  common = {
    inherit (workspace) cargoVendorDir;
    strictDeps = true;
    CARGO_PROFILE = "wasm";
    cargoExtraArgs = "--locked -p chlive-wasm --target wasm32-unknown-unknown";
    doCheck = false;
  };

  # chlive-wasm-deps-0: a fixed version, so a member's version bump doesn't rebuild them
  cargoArtifacts = craneLib.buildDepsOnly (
    common
    // {
      pname = "chlive-wasm";
      version = "0";
      inherit (workspace) dummySrc;
      cargoCheckCommand = "true";
    }
  );
in
craneLib.mkCargoDerivation (
  common
  // {
    pname = "chlive-wasm";
    version = (lib.importTOML ../../wasm/Cargo.toml).package.version;
    inherit (workspace) src;
    inherit cargoArtifacts;

    nativeBuildInputs = [
      wasm-bindgen-cli
      binaryen
      removeReferencesTo
    ];

    # as the `wasm` script (nix/shell.nix) builds it
    buildPhaseCargoCommand = "cargoWithProfile build $cargoExtraArgs";
    doInstallCargoArtifacts = false;
    disallowedReferences = [
      workspace.cargoVendorDir
      craneLib.rustc
    ];
    installPhaseCommand = ''
      wasm-bindgen --target web --out-dir $out target/wasm32-unknown-unknown/wasm/chlive_wasm.wasm
      wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
        -o $out/chlive_wasm_bg.wasm $out/chlive_wasm_bg.wasm
      # panic locations name the vendored crates' and the standard library's sources: not a
      # reason to keep them (or the toolchain) alive
      remove-references-to -t ${workspace.cargoVendorDir} -t ${craneLib.rustc} $out/chlive_wasm_bg.wasm
    '';

    passthru.deps = cargoArtifacts;

    meta = {
      description = "chlive-core for the TS client, as WebAssembly (@chlive/client's wasm/ directory)";
      license = lib.licenses.mit;
    };
  }
)
