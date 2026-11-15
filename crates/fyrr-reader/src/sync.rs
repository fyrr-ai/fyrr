//! Fatal poison handling for reader mutexes. Fallible authorization locks stay local.
use std::sync::{Mutex, MutexGuard};

#[track_caller]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("reader mutex poisoned")
}
