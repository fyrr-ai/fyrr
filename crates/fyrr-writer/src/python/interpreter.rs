//! Finds the Python interpreter that hosts the connector providers.
//!
//! In this order: `$VIRTUAL_ENV/bin/python`; a `python3` next to the `fyrr`
//! executable (a `fyrr` installed into a Python environment uses that
//! environment); `python3` on `PATH`. Only absolute locations are considered:
//! an empty or relative `PATH` entry would mean the current directory, and an
//! interpreter is never run from there.

use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const FIX: &str = "install the connector SDK and the providers you need with \
`pip install fyrr-connectors[<type>]` into a Python 3.13+ environment, and set VIRTUAL_ENV to \
it (or put that environment's python3 on PATH)";

/// How long the import probe may take before it is abandoned.
const PROBE_TIMEOUT: Duration = Duration::from_secs(60);

fn executable(path: &Path) -> bool {
    path.metadata()
        .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

/// The interpreter selected by the three places, from explicit inputs.
fn select(
    virtual_env: Option<OsString>,
    executable_dir: Option<&Path>,
    path: Option<OsString>,
) -> Result<PathBuf, String> {
    if let Some(venv) = virtual_env.filter(|venv| !venv.is_empty()) {
        let python = Path::new(&venv).join("bin/python");
        return if python.is_absolute() && executable(&python) {
            Ok(python)
        } else {
            Err(format!(
                "VIRTUAL_ENV ({}) is not an absolute path to a Python environment with bin/python; \
                 {FIX}",
                Path::new(&venv).display()
            ))
        };
    }
    let beside = executable_dir.map(|dir| dir.join("python3"));
    let on_path = path
        .iter()
        .flat_map(std::env::split_paths)
        .map(|dir| dir.join("python3"));
    beside
        .into_iter()
        .chain(on_path)
        .find(|candidate| candidate.is_absolute() && executable(candidate))
        .ok_or_else(|| format!("no Python interpreter found for the connector providers; {FIX}"))
}

pub(super) fn resolve() -> Result<PathBuf, String> {
    let executable_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    select(
        std::env::var_os("VIRTUAL_ENV"),
        executable_dir.as_deref(),
        std::env::var_os("PATH"),
    )
}

/// The interpreter, once it is known to import the connector SDK.
pub(crate) fn preflight() -> Result<PathBuf, String> {
    let python = resolve()?;
    let missing = || {
        format!(
            "the connector SDK (fyrr_connectors, with pyarrow) cannot be imported by {}; {FIX}",
            python.display()
        )
    };
    let mut probe = Command::new(&python)
        .args(["-P", "-c", "import fyrr_connectors.runtime"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| missing())?;
    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        match probe.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(python),
            Ok(Some(_)) | Err(_) => return Err(missing()),
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(None) => {
                let _ = probe.kill();
                let _ = probe.wait();
                return Err(missing());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake(dir: &Path, name: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("fyrr-interpreter-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn the_three_places_are_tried_in_order() {
        let root = scratch("order");
        let venv = fake(&root.join("venv/bin"), "python");
        let beside = fake(&root.join("exe"), "python3");
        let on_path = fake(&root.join("path"), "python3");
        let path = Some(root.join("path").into_os_string());
        let exe = Some(root.join("exe"));
        let venv_dir = || Some(root.join("venv").into_os_string());
        assert_eq!(
            select(venv_dir(), exe.as_deref(), path.clone()).unwrap(),
            venv
        );
        assert_eq!(select(None, exe.as_deref(), path.clone()).unwrap(), beside);
        assert_eq!(
            select(Some(OsString::new()), exe.as_deref(), path.clone()).unwrap(),
            beside
        );
        assert_eq!(select(None, None, path).unwrap(), on_path);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_relative_location_is_never_run() {
        let root = scratch("relative");
        fake(&root, "python3");
        // A directory that exists and holds an executable python3, spelled
        // relative to the current directory; empty entries mean "here".
        let up = std::env::current_dir().unwrap().components().count() - 1;
        let relative = (0..up)
            .fold(PathBuf::new(), |path, _| path.join(".."))
            .join(root.strip_prefix("/").unwrap());
        assert!(executable(&relative.join("python3")));
        let by_path = select(None, None, Some(relative.clone().into_os_string())).unwrap_err();
        let empty = select(None, None, Some(OsString::from(":"))).unwrap_err();
        let by_venv = select(Some(relative.into_os_string()), None, None).unwrap_err();
        assert!(by_path.contains("no Python interpreter"), "{by_path}");
        assert!(empty.contains("no Python interpreter"), "{empty}");
        assert!(by_venv.contains("VIRTUAL_ENV"), "{by_venv}");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn the_messages_name_the_fix() {
        let none = select(None, None, None).unwrap_err();
        assert!(
            none.contains("pip install fyrr-connectors[<type>]"),
            "{none}"
        );
        assert!(none.contains("VIRTUAL_ENV"), "{none}");
        let empty = scratch("empty");
        std::fs::create_dir_all(&empty).unwrap();
        let broken = select(Some(empty.clone().into_os_string()), None, None).unwrap_err();
        assert!(
            broken.contains("pip install fyrr-connectors[<type>]"),
            "{broken}"
        );
        std::fs::remove_dir_all(empty).unwrap();
    }
}
