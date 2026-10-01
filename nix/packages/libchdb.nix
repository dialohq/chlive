# chDB (embedded ClickHouse) as a shared library; not in nixpkgs. Its engine version MUST
# match main ClickHouse (./clickhouse.nix): aggregate states cross between them in binary form.
{
  lib,
  stdenv,
  fetchurl,
  autoPatchelfHook,
}:
let
  version = "26.7.3"; # = ClickHouse 26.7.2.1
  assets = {
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
  asset = assets.${stdenv.hostPlatform.system};
  inherit (stdenv.hostPlatform) isLinux;
in
stdenv.mkDerivation {
  pname = "libchdb";
  inherit version;
  src = fetchurl {
    url = "https://github.com/chdb-io/chdb-core/releases/download/v${version}/${asset.name}-libchdb.tar.gz";
    inherit (asset) sha256;
  };
  sourceRoot = ".";
  nativeBuildInputs = lib.optionals isLinux [ autoPatchelfHook ];
  buildInputs = lib.optionals isLinux [ stdenv.cc.cc.lib ];
  dontConfigure = true;
  dontBuild = true;
  dontStrip = true;
  installPhase = ''
    install -Dm644 libchdb.so $out/lib/libchdb.so
    install -Dm644 chdb.h $out/include/chdb.h
  '';
}
