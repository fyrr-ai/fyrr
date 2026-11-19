//! The one place that writes this process's environment.
//!
//! Lance sizes several pools and layouts from environment variables that it
//! reads once per process, so a setting can only take effect when the
//! variable is set before the first Lance call. Both executables set theirs
//! here, once, from their start-up path.

/// Sets environment variables for the rest of the process.
///
/// Call it only from the start-up path, before the first Tokio runtime, Lance
/// call or other thread of the process.
#[allow(unsafe_code)]
pub fn set_at_startup(variables: impl IntoIterator<Item = (&'static str, String)>) {
    for (name, value) in variables {
        // SAFETY: the process is still single-threaded here (the callers run
        // before any runtime or worker thread starts), so no other thread
        // can read the environment concurrently.
        unsafe { std::env::set_var(name, value) };
    }
}
