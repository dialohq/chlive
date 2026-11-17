{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    # the Rust toolchain, with the wasm32 target nixpkgs' rustc lacks (the TS client's core)
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    # its `feat/rex-runner` branch (the Rex runner, `dnvr down`/`restart`) until that's merged
    dnvr.url = "github:dialohq/dnvr/feat/rex-runner";
    dnvr.inputs.nixpkgs.follows = "nixpkgs";
    # the server's OCI image (packages.chlive-image)
    nix2container.url = "github:nlewo/nix2container";
    nix2container.inputs.nixpkgs.follows = "nixpkgs";
    # the Rust builds: dependencies built once, apart from the workspace's own crates (it has no
    # inputs, so nothing to follow)
    crane.url = "github:ipetkov/crane";
    # The production ingestion (Kafka Connect's ClickHouse sink, exactly once) and its pause
    # server, which chlive's snapshots go through. Its `pause-server` branch (the pause server's
    # Keeper storage and leader election) until that's merged into main.
    continuous-backup = {
      url = "github:dialohq/clickhouse-continuous-backup/pause-server";
      flake = false;
    };
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      imports = [ inputs.dnvr.flakeModule ];

      perSystem =
        {
          pkgs,
          presets,
          dnvrState,
          system,
          ...
        }:
        let
          clickhouse = pkgs.callPackage ./nix/packages/clickhouse.nix { };
          # chDB (ClickHouse as a library), embedded in the engine to merge aggregate states;
          # built from the same ClickHouse version as ./clickhouse.nix.
          libchdb = pkgs.callPackage ./nix/packages/libchdb.nix { };
          # the server (`chlive`) and the terminal client (`clc`), on nixpkgs' rustc
          chlive = pkgs.callPackage ./nix/packages/chlive.nix {
            inherit libchdb;
            craneLib = inputs.crane.mkLib pkgs;
          };
          # wasm-bindgen's CLI at the `wasm-bindgen` crate's version (the dev shells use it too)
          wasm-bindgen-cli = pkgs.callPackage ./nix/packages/wasm-bindgen-cli.nix { };
          # chlive-core for the TS client, as WebAssembly: @chlive/client's wasm/ directory
          chlive-wasm = pkgs.callPackage ./nix/packages/chlive-wasm.nix {
            inherit wasm-bindgen-cli;
            craneLib = (inputs.crane.mkLib pkgs).overrideToolchain (import ./nix/rust.nix { inherit pkgs; });
          };
          # the server as an OCI image
          chlive-image = pkgs.callPackage ./nix/packages/chlive-image.nix {
            inherit chlive libchdb;
            inherit (inputs.nix2container.packages.${system}) nix2container;
          };
        in
        {
          # nixpkgs with rust-overlay's `rust-bin` (nothing else changes: same packages)
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [ inputs.rust-overlay.overlays.default ];
          };
          formatter = pkgs.nixfmt;
          packages = {
            inherit
              clickhouse
              libchdb
              chlive
              chlive-wasm
              ;
            default = chlive;
          }
          // pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux { inherit chlive-image; };

          # Cargo.lock's wasm-bindgen must be the CLI's: chlive-wasm's JS and module are made by both
          checks.wasm-bindgen-version =
            let
              locked =
                (pkgs.lib.findFirst (p: p.name == "wasm-bindgen") null (pkgs.lib.importTOML ./Cargo.lock).package)
                .version;
            in
            pkgs.runCommand "wasm-bindgen-version" { } (
              if locked == wasm-bindgen-cli.version then
                "touch $out"
              else
                ''
                  echo "Cargo.lock has wasm-bindgen ${locked}, but nix/packages/wasm-bindgen-cli.nix builds ${wasm-bindgen-cli.version}: make them the same" >&2
                  exit 1
                ''
            );

          dnvr.shells.default = import ./nix/shell.nix {
            inherit
              pkgs
              presets
              dnvrState
              clickhouse
              libchdb
              ;
            inherit (inputs) continuous-backup;
          };
          # the chat demo (examples/chat): `nix develop .#chat`
          dnvr.shells.chat = import ./nix/chat.nix {
            inherit
              pkgs
              presets
              dnvrState
              clickhouse
              libchdb
              ;
            inherit (inputs) continuous-backup;
          };
        };
    };
}
