# The Cargo workspace for crane: its source, and the dummy source and vendored crates the
# dependency builds (buildDepsOnly) use. Those must change only with Cargo.lock's dependencies, so
# the members' versions are left out of both: a version bump rebuilds chlive, not its dependencies.
{ lib, craneLib }:
let
  root = ../..;
  members = [
    ../../server
    ../../core
    ../../engine
    ../../wasm
    ../../protocol
    ../../client-rs
    ../../tests
    ../../examples/rust-alerts
    ../../tui
  ];
  packages = map (m: (lib.importTOML (m + "/Cargo.toml")).package) members;

  # Cargo.lock with every member at 0.0.0 (what cargo takes for a Cargo.toml without a version)
  lock =
    builtins.replaceStrings
      (map (p: ''
        name = "${p.name}"
        version = "${p.version}"
      '') packages)
      (map (p: ''
        name = "${p.name}"
        version = "0.0.0"
      '') packages)
      (builtins.readFile ../../Cargo.lock);
in
{
  # The whole workspace: cargo resolves every member, even when building one.
  src = lib.fileset.toSource {
    inherit root;
    fileset = lib.fileset.unions (
      [
        ../../Cargo.toml
        ../../Cargo.lock
      ]
      ++ members
    );
  };

  cargoVendorDir = craneLib.vendorCargoDeps { cargoLockContents = lock; };

  # the Cargo.toml files without their versions, stub sources, and the lock above
  dummySrc = craneLib.mkDummySrc {
    src = lib.fileset.toSource {
      inherit root;
      fileset = lib.fileset.unions ([ ../../Cargo.toml ] ++ map (m: m + "/Cargo.toml") members);
    };
    cargoLock = builtins.toFile "Cargo.lock" lock;
    cleanCargoTomlFilter =
      path:
      path != [
        "package"
        "version"
      ]
      && craneLib.filters.cargoTomlDefault path;
  };
}
