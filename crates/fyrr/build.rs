//! Development builds locate libpython through an interpreter-specific rpath.
//! Release builds use the runtime loader's search path.
//! Reader-only builds link no Python.

#[allow(clippy::print_stdout)]
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    #[cfg(feature = "writer")]
    if std::env::var("PROFILE").as_deref() != Ok("release") {
        pyo3_build_config::add_libpython_rpath_link_args();
    }
}
