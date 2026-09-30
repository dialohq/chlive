{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      perSystem = { pkgs, ... }: {
        formatter = pkgs.nixfmt;

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            nixfmt

            # Rust toolchain
            cargo
            rustc
            clippy
            rustfmt
            rust-analyzer

            # native deps for the rdkafka crate (dynamic-linking feature)
            pkg-config
            rdkafka
            zlib

            # client / demo
            nodejs_24

            # debugging helpers (rpk is unfree in nixpkgs; use `docker compose exec redpanda rpk`)
            jq
            curl
          ];

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
        };
      };
    };
}
