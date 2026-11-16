//! Wall time for persisted timestamps and validity decisions. Monotonic timers,
//! Tokio scheduling and external identity-provider clocks remain independent.

use chrono::{DateTime, Utc};

#[cfg(all(feature = "test-clock", not(debug_assertions)))]
compile_error!("test-clock must not be enabled in shipping builds");

/// The real wall clock, with a forward-only offset in instrumented test builds.
pub fn now() -> DateTime<Utc> {
    #[allow(clippy::disallowed_methods)]
    let real: DateTime<Utc> = std::time::SystemTime::now().into();
    #[cfg(feature = "test-clock")]
    {
        real + chrono::Duration::milliseconds(test_clock::offset())
    }
    #[cfg(not(feature = "test-clock"))]
    real
}

#[cfg(feature = "test-clock")]
pub use test_clock::advance;

#[cfg(any(test, feature = "test-clock"))]
mod test_clock {
    use std::io;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    #[cfg(feature = "test-clock")]
    static OFFSET: AtomicI64 = AtomicI64::new(0);

    fn read(path: &Path, previous: &AtomicI64) -> io::Result<i64> {
        let value: i64 = std::fs::read_to_string(path)?
            .trim()
            .parse()
            .map_err(|_| io::Error::other("invalid test clock offset"))?;
        if !(0..=chrono::Duration::days(3650).num_milliseconds()).contains(&value) {
            return Err(io::Error::other("test clock offset is out of range"));
        }
        Ok(previous.fetch_max(value, Ordering::SeqCst).max(value))
    }

    #[cfg(feature = "test-clock")]
    pub(super) fn offset() -> i64 {
        let Some(path) = std::env::var_os("FYRR_TEST_CLOCK_FILE") else {
            return 0;
        };
        read(Path::new(&path), &OFFSET)
            .unwrap_or_else(|_| panic!("cannot read the test clock offset"))
    }

    fn advance_file(path: &Path, previous: &AtomicI64, duration: Duration) -> io::Result<()> {
        let millis = i64::try_from(duration.as_millis())
            .map_err(|_| io::Error::other("test clock advance is out of range"))?;
        let value = read(path, previous)?
            .checked_add(millis)
            .filter(|value| *value <= chrono::Duration::days(3650).num_milliseconds())
            .ok_or_else(|| io::Error::other("test clock advance is out of range"))?;
        // One controller per fixture writes; all child processes only read.
        // Rename makes every read see one complete offset.
        let temporary = path.with_extension(format!("{}-tmp", std::process::id()));
        std::fs::write(&temporary, value.to_string())?;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        std::fs::rename(&temporary, path)?;
        previous.fetch_max(value, Ordering::SeqCst);
        Ok(())
    }

    /// Advance this fixture's wall time. Sleeps and operation timeouts stay real.
    #[cfg(feature = "test-clock")]
    pub fn advance(duration: Duration) -> io::Result<()> {
        let path = std::env::var_os("FYRR_TEST_CLOCK_FILE")
            .ok_or_else(|| io::Error::other("test clock requires scripts/ci"))?;
        advance_file(Path::new(&path), &OFFSET, duration)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn offset_only_moves_forward_and_is_shared_through_a_file() {
            let root = std::env::temp_dir().join(format!(
                "fyrr-clock-{}-{}",
                std::process::id(),
                super::super::now().timestamp_nanos_opt().unwrap()
            ));
            std::fs::create_dir(&root).unwrap();
            let path = root.join("offset");
            let parent = AtomicI64::new(0);
            let child = AtomicI64::new(0);
            std::fs::write(&path, "0").unwrap();
            assert_eq!(read(&path, &parent).unwrap(), 0);
            advance_file(&path, &parent, Duration::from_secs(5)).unwrap();
            assert_eq!(read(&path, &child).unwrap(), 5000);
            std::fs::write(&path, "2").unwrap();
            assert_eq!(read(&path, &parent).unwrap(), 5000);
            advance_file(&path, &parent, Duration::from_secs(7)).unwrap();
            assert_eq!(read(&path, &child).unwrap(), 12000);
            for invalid in ["-1", "bad", "99999999999999999999999"] {
                std::fs::write(&path, invalid).unwrap();
                assert!(read(&path, &parent).is_err());
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
