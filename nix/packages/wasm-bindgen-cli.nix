# wasm-bindgen's CLI at the version the `wasm-bindgen` crate is pinned to (wasm/Cargo.toml), for
# chlive-wasm and the dev shells; checks.wasm-bindgen-version fails when Cargo.lock's differs.
# nixpkgs' wasm-bindgen-cli_0_2_127, built here so it stays when nixpkgs drops that attribute.
{
  buildWasmBindgenCli,
  fetchCrate,
  rustPlatform,
}:
buildWasmBindgenCli rec {
  src = fetchCrate {
    pname = "wasm-bindgen-cli";
    version = "0.2.127";
    hash = "sha256-di+qBAdd7pENLiIB9CoZoab+W5xeDoByMREcCGTSzWo=";
  };

  cargoDeps = rustPlatform.fetchCargoVendor {
    inherit src;
    inherit (src) pname version;
    hash = "sha256-FTv2GZIAQs0ePdIZXIXil7JbZ6kIT05VG6vqC1qNFxQ=";
  };
}
