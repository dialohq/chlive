{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    dnvr.url = "github:dialohq/dnvr";
    dnvr.inputs.nixpkgs.follows = "nixpkgs";
    # The production ingestion (Kafka Connect's ClickHouse sink, exactly once) and its pause
    # server, which chlive's snapshots go through. A local checkout for now.
    continuous-backup = {
      url = "git+file:///Users/adam/continuous-backup";
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
        };
    };
}
