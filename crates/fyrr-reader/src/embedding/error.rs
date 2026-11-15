//! Closed embedding failures shared by the reader and writer adapters.

macro_rules! errors {
    ($($variant:ident => $wire:literal),* $(,)?) => {
        /// A fixed diagnostic; provider messages never enter this type.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum EmbeddingError { $($variant),* }

        impl EmbeddingError {
            pub const ALL: &'static [Self] = &[$(Self::$variant),*];

            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire),* }
            }
        }

        impl From<EmbeddingError> for crate::error::ReaderError {
            fn from(error: EmbeddingError) -> Self {
                match error { $(EmbeddingError::$variant => Self::$variant),* }
            }
        }
    };
}

errors! {
    EmbeddingConfigurationInvalid => "embedding_configuration_invalid",
    EmbeddingSpendLimitRequired => "embedding_spend_limit_required",
    EmbeddingSpendConfigurationInvalid => "embedding_spend_configuration_invalid",
    EmbeddingSpendLimitExceeded => "embedding_spend_limit_exceeded",
    EmbeddingCredentialsUnavailable => "embedding_credentials_unavailable",
    EmbeddingMemoryBudgetExhausted => "embedding_memory_budget_exhausted",
    EmbeddingTimeout => "embedding_timeout",
    EmbeddingProviderUnavailable => "embedding_provider_unavailable",
    EmbeddingResponseInvalid => "embedding_response_invalid",
    EmbeddingModelMismatch => "embedding_model_mismatch",
    EmbeddingBatchInvalid => "embedding_batch_invalid",
    EmbeddingInvalid => "embedding_invalid",
    EmbeddingProfileInvalid => "embedding_profile_invalid",
    EmbeddingProfileMismatch => "embedding_profile_mismatch",
    EmbeddingDimensionMismatch => "embedding_dimension_mismatch",
    EmbeddingUnavailable => "embedding_unavailable",
}

impl std::fmt::Display for EmbeddingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
impl std::error::Error for EmbeddingError {}
