use std::path::PathBuf;

fn main() {
    let out = std::env::var("OUT_DIR").unwrap();
    println!("cargo:rustc-link-search={out}");
    let src = "lmdb/libraries/liblmdb";
    cc::Build::new()
        .file(format!("{src}/mdb.c"))
        .file(format!("{src}/midl.c"))
        .compile("lmdb");
    println!("cargo:rustc-link-lib=lmdb");
    let bindings = bindgen::Builder::default()
        .header(format!("{src}/lmdb.h"))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .unwrap();
    bindings
        .write_to_file(PathBuf::from(&out).join("lmdb.rs"))
        .unwrap();
}
