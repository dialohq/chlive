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
          # the server (`chlive`) and the terminal client (`clc`)
          chlive = pkgs.callPackage ./nix/packages/chlive.nix { inherit libchdb; };
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
              ;
            default = chlive;
          };

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
