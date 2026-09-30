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

      perSystem =
        { pkgs, system, ... }:
        let
          # chDB (embedded ClickHouse) is not in nixpkgs. Its engine version MUST match the
          # main ClickHouse server in docker-compose.yml: aggregate states cross between them.
          libchdbVersion = "26.7.3";
          libchdbAssets = {
            aarch64-darwin = {
              name = "macos-arm64";
              sha256 = "5640e50dccf711bf3dd5551333d08e43f433edf7bd94b2289f36c2539e627762";
            };
            x86_64-linux = {
              name = "linux-x86_64";
              sha256 = "bc33260c32acf78eade2ac41a9115f38e00404651fa42e3bb4c419e4f011c031";
            };
            aarch64-linux = {
              name = "linux-aarch64";
              sha256 = "d153adad1ff39b2e3caf0417f09d8bd9edd41939c7c67a3c4978a61e73fb9227";
            };
          };
          libchdb = pkgs.stdenv.mkDerivation {
            pname = "libchdb";
            version = libchdbVersion;
            src = pkgs.fetchurl {
              url = "https://github.com/chdb-io/chdb-core/releases/download/v${libchdbVersion}/${
                libchdbAssets.${system}.name
              }-libchdb.tar.gz";
              inherit (libchdbAssets.${system}) sha256;
            };
            sourceRoot = ".";
            nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.autoPatchelfHook ];
            buildInputs = pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.stdenv.cc.cc.lib ];
            dontConfigure = true;
            dontBuild = true;
            dontStrip = true;
            installPhase = ''
              install -Dm644 libchdb.so $out/lib/libchdb.so
              install -Dm644 chdb.h $out/include/chdb.h
            '';
          };
        in
        {
          formatter = pkgs.nixfmt;
          packages.libchdb = libchdb;

          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              nixfmt

              # Rust toolchain
              cargo
              rustc
              clippy
              rustfmt
              rust-analyzer

              # embedded cache: chdb-rust links libchdb; its bindgen needs libclang
              libchdb
              llvmPackages.libclang

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
            # chdb-rust links against these instead of downloading an engine at build time.
            CHDB_LIB_DIR = "${libchdb}/lib";
            CHDB_INCLUDE_DIR = "${libchdb}/include";
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          };
        };
    };
}
