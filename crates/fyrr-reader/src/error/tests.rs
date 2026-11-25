use super::*;
use crate::storage::StorageError as S;

#[test]
fn every_reader_error_preserves_legacy_mcp_mapping() {
    fn old_lookup(code: &str) -> ErrorCode {
        ErrorCode::ALL
            .into_iter()
            .find(|value| value.as_str() == code)
            .unwrap_or(ErrorCode::StorageUnavailable)
    }
    for &error in ReaderError::ALL {
        assert_eq!(old_lookup(error.as_str()), error.mcp_code(), "{error}");
    }
    for &error in crate::embedding::EmbeddingError::ALL {
        let mapped = ReaderError::from(error);
        assert_eq!(mapped.as_str(), error.as_str());
        assert_eq!(mapped.mcp_code(), ErrorCode::StorageUnavailable);
    }
    for (storage, reader, wire) in [
        (
            S::StorageConfigurationInvalid,
            ReaderError::StorageConfigurationInvalid,
            "storage_configuration_invalid",
        ),
        (
            S::StorageUnavailable,
            ReaderError::StorageUnavailable,
            "storage_unavailable",
        ),
        (
            S::PublicationInvalid,
            ReaderError::PublicationInvalid,
            "publication_invalid",
        ),
        (
            S::AuthorizationUnavailable,
            ReaderError::AuthorizationUnavailable,
            "authorization_unavailable",
        ),
        (S::StaleCursor, ReaderError::StaleCursor, "stale_cursor"),
        (
            S::TooManyNamespaces,
            ReaderError::TooManyNamespaces,
            "too_many_namespaces",
        ),
        (
            S::StorageRootOld,
            ReaderError::StorageRootOld,
            "storage_root_old",
        ),
        (
            S::NamespaceLayoutOld,
            ReaderError::NamespaceLayoutOld,
            "namespace_layout_old",
        ),
        (
            S::PublicationUnavailable,
            ReaderError::PublicationUnavailable,
            "publication_unavailable",
        ),
    ] {
        assert_eq!(storage.as_str(), wire);
        assert_eq!(storage.to_string(), wire);
        assert_eq!(ReaderError::from(storage), reader);
        assert_eq!(reader.as_str(), wire);
        assert_eq!(old_lookup(wire), reader.mcp_code());
    }
    for (index, reader, wire) in [
        (
            IndexVerificationError::StorageUnavailable,
            ReaderError::StorageUnavailable,
            "storage_unavailable",
        ),
        (
            IndexVerificationError::ReadersIndexMissing,
            ReaderError::ReadersIndexMissing,
            "readers_index_missing",
        ),
        (
            IndexVerificationError::IncompatibleIndex,
            ReaderError::IncompatibleIndex,
            "incompatible_index",
        ),
    ] {
        assert_eq!(ReaderError::from(index), reader);
        assert_eq!(index.to_string(), wire);
        assert_eq!(reader.as_str(), wire);
        assert_eq!(reader.mcp_code(), old_lookup(wire));
    }
}
