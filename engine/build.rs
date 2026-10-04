// libchdb's install name is `@rpath/libchdb.so`; embed the Nix store path so binaries (incl.
// test binaries) run outside `cargo run` without DYLD_LIBRARY_PATH.
fn main() {
    println!("cargo:rerun-if-env-changed=CHDB_LIB_DIR");
    if let Ok(dir) = std::env::var("CHDB_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
}
