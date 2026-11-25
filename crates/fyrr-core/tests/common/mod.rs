//! Helpers shared by the fyrr-core integration tests.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use serde_json::Value;

/// The repository root (two levels above this crate).
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root exists")
}

pub fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub fn read_json(relative: &str) -> Value {
    serde_json::from_str(&read(relative))
        .unwrap_or_else(|error| panic!("parse {relative}: {error}"))
}

/// Sorted file names (relative to the repository root) in a directory.
pub fn json_files(relative_dir: &str) -> Vec<String> {
    let dir = repo_root().join(relative_dir);
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("list {}: {error}", dir.display()))
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .map(|path| {
            format!(
                "{relative_dir}/{}",
                path.file_name().expect("file name").to_string_lossy()
            )
        })
        .collect();
    files.sort();
    files
}

/// Validates `instance` against a committed JSON Schema.
pub fn assert_valid(schema_path: &str, instance: &Value, what: &str) {
    let schema = read_json(schema_path);
    let validator = jsonschema::validator_for(&schema)
        .unwrap_or_else(|error| panic!("{schema_path} is not a valid JSON Schema: {error}"));
    let errors: Vec<String> = validator
        .iter_errors(instance)
        .map(|error| format!("{} at {}", error, error.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "{what} does not match {schema_path}: {errors:#?}"
    );
}

/// Calls `$check::<T>()` once for every stored contract type and yields the
/// results in an array: the one inventory of the schema, golden-fixture and
/// version checks. A fixture directory and a schema are named after `T::NAME`.
#[allow(unused_macros)]
macro_rules! for_each_contract {
    ($check:ident) => {
        [
            $check::<fyrr_core::connector::ConnectorBatch>(),
            $check::<fyrr_core::connector::CursorState>(),
            $check::<fyrr_core::envelope::PreparedEnvelope>(),
            $check::<fyrr_core::envelope::RejectionMarker>(),
            $check::<fyrr_core::publication::ContentManifest>(),
            $check::<fyrr_core::publication::ServingHead>(),
            $check::<fyrr_core::publication::PolicyArtifact>(),
            $check::<fyrr_core::publication::EnvelopeCheckpoint>(),
            $check::<fyrr_core::publication::HistoryCut>(),
            $check::<fyrr_core::identity::IdentityArtifact>(),
            $check::<fyrr_core::identity::ApiKeysArtifact>(),
            $check::<fyrr_core::directory::DirectorySnapshot>(),
            $check::<fyrr_core::secret::EncryptedSecret>(),
            $check::<fyrr_core::provider::ProviderInfo>(),
        ]
    };
}
#[allow(unused_imports)]
pub(crate) use for_each_contract;
