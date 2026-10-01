# ClickHouse for both dev shells, pinned (nixpkgs has 26.5).
# Official release binaries: a single `clickhouse` multi-tool.
{
  stdenvNoCC,
  fetchurl,
}:
let
  version = "26.7.2.59";
  base = "https://github.com/ClickHouse/ClickHouse/releases/download/v${version}-stable";
  assets = {
    aarch64-darwin = {
      url = "${base}/clickhouse-macos-aarch64";
      sha256 = "003gy3c5r4hprakgd071qgyir4z4jliwb7mdlir03yn5lkdasl2d";
    };
    x86_64-linux = {
      url = "${base}/clickhouse-common-static-${version}-amd64.tgz";
      sha512 = "cafbb6a6651ee72210d2ee6bb67c1c2b4d2aee8cf6197df02c8323f674213c335e114acacc94e70190dfb77bbb5e82869f6e08c4eb3058f8972b12f86ed95a1a";
    };
    aarch64-linux = {
      url = "${base}/clickhouse-common-static-${version}-arm64.tgz";
      sha512 = "6945f48dc72f539072d2e102abfb806dac2f65d80d7f78e4b646cd1aa451433fd84d91a4551fc26366fec9414f44a239d9e5fa40942868fc80e7be909f0a7a25";
    };
  };
in
stdenvNoCC.mkDerivation {
  pname = "clickhouse";
  inherit version;
  src = fetchurl assets.${stdenvNoCC.hostPlatform.system};
  dontUnpack = true;
  dontStrip = true;
  installPhase = ''
    mkdir -p $out/bin
    ${
      if stdenvNoCC.hostPlatform.isLinux then
        ''
          tar xzf $src
          install -m755 "$(find . -path '*/bin/clickhouse' -type f | head -1)" $out/bin/clickhouse
        ''
      else
        "install -m755 $src $out/bin/clickhouse"
    }
    # Release binaries are self-extracting: the first run decompresses the binary in
    # place, which must happen here while $out is still writable.
    HOME=$TMPDIR $out/bin/clickhouse --version
    for tool in server client local; do ln -s clickhouse $out/bin/clickhouse-$tool; done
  '';
}
