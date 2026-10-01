{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    dnvr.url = "github:dialohq/dnvr";
    dnvr.inputs.nixpkgs.follows = "nixpkgs";
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
        { pkgs, dnvrState, ... }:
        let
          settings = import ./nix/settings.nix;
          libchdb = pkgs.callPackage ./nix/packages/libchdb.nix { };
          clickhouse = pkgs.callPackage ./nix/packages/clickhouse.nix { };
        in
        {
          formatter = pkgs.nixfmt;
          packages = { inherit libchdb clickhouse; };

          # Available as `chlive` in every module of the dnvr shell (./nix/shell).
          dnvr.specialArgs.chlive = {
            inherit settings libchdb clickhouse;
            redpanda = pkgs.callPackage ./nix/redpanda.nix { inherit dnvrState settings; };
            chClient = import ./nix/clickhouse-client.nix {
              inherit (pkgs) writeText;
              inherit settings;
            };
          };

          dnvr.shells.default = ./nix/shell;
        };
    };
}
