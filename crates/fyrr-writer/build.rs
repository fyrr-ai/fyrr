//! Lets this crate's test binaries find libpython at run time (PyO3 links it
//! dynamically; see crates/fyrr/build.rs).

fn main() {
    pyo3_build_config::add_libpython_rpath_link_args();
}
