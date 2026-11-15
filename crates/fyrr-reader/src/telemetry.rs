//! Fixed operator event text. Arguments are borrowed until the synchronous write ends.
#[inline]
#[allow(clippy::print_stderr)]
pub(crate) fn emit(event: std::fmt::Arguments<'_>) {
    eprintln!("{event}");
}

pub(crate) fn micros(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
