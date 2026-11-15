//! The full build links libpython dynamically through PyO3. An rpath
//! to the interpreter's library directory lets development binaries run
//! without LD_LIBRARY_PATH; in the image it is /usr/local/lib, already on the
//! loader path. The reader-only build has no Python and no rpath.

fn main() {
    #[cfg(feature = "writer")]
    pyo3_build_config::add_libpython_rpath_link_args();
}
