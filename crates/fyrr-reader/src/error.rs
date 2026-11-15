//! Closed reader diagnostics and their stable MCP representation.
use fyrr_core::mcp::ErrorCode;

/// The complete failure set of native index verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexVerificationError {
    StorageUnavailable,
    ReadersIndexMissing,
    IncompatibleIndex,
}

macro_rules! errors {
    ($($(#[$attr:meta])* $variant:ident => (
        $text:literal, $wire:ident $(,)?)),* $(,)?) => {
        /// Closed reader failure. Only its fixed code crosses a process boundary.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum ReaderError { $($(#[$attr])* $variant),* }
        impl ReaderError {
            #[cfg(test)]
            pub(crate) const ALL: &'static [Self] = &[$($(#[$attr])* Self::$variant),*];
            pub const fn as_str(self) -> &'static str {
                match self { $($(#[$attr])* Self::$variant => $text),* }
            }
            pub(crate) const fn mcp_code(self) -> ErrorCode {
                match self { $($(#[$attr])* Self::$variant => ErrorCode::$wire),* }
            }
        }
    };
}

errors! {
    AuthorizationUnavailable => ("authorization_unavailable", AuthorizationUnavailable),
    Busy => ("busy", Busy),
    Cancelled => ("cancelled", Cancelled),
    ConfigurationInvalid => ("configuration_invalid", StorageUnavailable),
    DeadlineExceeded => ("deadline_exceeded", DeadlineExceeded),
    EmbeddingBatchInvalid => ("embedding_batch_invalid", StorageUnavailable),
    EmbeddingConfigurationInvalid => ("embedding_configuration_invalid", StorageUnavailable),
    EmbeddingCredentialsUnavailable => ("embedding_credentials_unavailable", StorageUnavailable),
    EmbeddingDimensionMismatch => ("embedding_dimension_mismatch", StorageUnavailable),
    EmbeddingInvalid => ("embedding_invalid", StorageUnavailable),
    EmbeddingMemoryBudgetExhausted => ("embedding_memory_budget_exhausted", StorageUnavailable),
    EmbeddingModelMismatch => ("embedding_model_mismatch", StorageUnavailable),
    EmbeddingProfileInvalid => ("embedding_profile_invalid", StorageUnavailable),
    EmbeddingProfileMismatch => ("embedding_profile_mismatch", StorageUnavailable),
    EmbeddingProviderUnavailable => ("embedding_provider_unavailable", StorageUnavailable),
    EmbeddingResponseInvalid => ("embedding_response_invalid", StorageUnavailable),
    EmbeddingSpendConfigurationInvalid => (
        "embedding_spend_configuration_invalid",
        StorageUnavailable,
    ),
    EmbeddingSpendLimitExceeded => ("embedding_spend_limit_exceeded", StorageUnavailable),
    EmbeddingSpendLimitRequired => ("embedding_spend_limit_required", StorageUnavailable),
    EmbeddingTimeout => ("embedding_timeout", StorageUnavailable),
    EmbeddingUnavailable => ("embedding_unavailable", StorageUnavailable),
    // HTTP only: the `Host` header is not the authority of `public_url`.
    HostMismatch => ("host_mismatch", StorageUnavailable),
    IncompatibleIndex => ("incompatible_index", IncompatibleIndex),
    IndexRowsInvalid => ("index_rows_invalid", StorageUnavailable),
    InvalidArgument => ("invalid_argument", InvalidArgument),
    MirrorCapacityExhausted => ("mirror_capacity_exhausted", StorageUnavailable),
    PartitionMapUnavailable => ("partition_map_unavailable", StorageUnavailable),
    PreparationBudgetExhausted => ("preparation_budget_exhausted", StorageUnavailable),
    PreparationBudgetImpossible => ("preparation_budget_impossible", StorageUnavailable),
    PreparationDeadline => ("preparation_deadline", StorageUnavailable),
    PreparationSuperseded => ("preparation_superseded", StorageUnavailable),
    PublicationInvalid => ("publication_invalid", StorageUnavailable),
    PublicationRowsInvalid => ("publication_rows_invalid", StorageUnavailable),
    PublicationUnavailable => ("publication_unavailable", StorageUnavailable),
    RateLimited => ("rate_limited", RateLimited),
    ReadersIndexMissing => ("readers_index_missing", StorageUnavailable),
    RerankDeadline => ("rerank_deadline", StorageUnavailable),
    RerankPayloadLimit => ("rerank_payload_limit", StorageUnavailable),
    RerankProviderFailed => ("rerank_provider_failed", StorageUnavailable),
    RerankResponseInvalid => ("rerank_response_invalid", StorageUnavailable),
    RerankSpendExhausted => ("rerank_spend_exhausted", StorageUnavailable),
    SearchResultInvalid => ("search_result_invalid", StorageUnavailable),
    SerializationFailed => ("serialization_failed", StorageUnavailable),
    StaleCursor => ("stale_cursor", StaleCursor),
    StorageConfigurationInvalid => ("storage_configuration_invalid", StorageUnavailable),
    StorageUnavailable => ("storage_unavailable", StorageUnavailable),
    TransportUnavailable => ("transport_unavailable", StorageUnavailable),
    Unauthorized => ("unauthorized", StorageUnavailable),
}

pub(crate) use ReaderError::*;
pub(crate) type Result<T, E = ReaderError> = std::result::Result<T, E>;

impl From<IndexVerificationError> for ReaderError {
    fn from(error: IndexVerificationError) -> Self {
        match error {
            IndexVerificationError::StorageUnavailable => Self::StorageUnavailable,
            IndexVerificationError::ReadersIndexMissing => Self::ReadersIndexMissing,
            IndexVerificationError::IncompatibleIndex => Self::IncompatibleIndex,
        }
    }
}

impl std::fmt::Display for IndexVerificationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&ReaderError::from(*self), formatter)
    }
}
impl std::error::Error for IndexVerificationError {}

impl From<crate::storage::StorageError> for ReaderError {
    fn from(error: crate::storage::StorageError) -> Self {
        use crate::storage::StorageError as S;
        match error {
            S::StorageConfigurationInvalid => Self::StorageConfigurationInvalid,
            S::StorageUnavailable => Self::StorageUnavailable,
            S::PublicationInvalid => Self::PublicationInvalid,
            S::AuthorizationUnavailable => Self::AuthorizationUnavailable,
            S::StaleCursor => Self::StaleCursor,
            S::PublicationUnavailable => Self::PublicationUnavailable,
        }
    }
}

impl std::fmt::Display for ReaderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
impl std::error::Error for ReaderError {}
#[cfg(test)]
#[path = "error/tests.rs"]
mod tests;
