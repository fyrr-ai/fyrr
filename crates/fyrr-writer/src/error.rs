//! Closed writer diagnostics and their existing wire spelling.

pub type Result<T, E = WriterError> = std::result::Result<T, E>;

macro_rules! writer_errors {
    ($($name:ident => ($wire:literal, $provider:literal)),* $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum WriterError { $($name),* }

        impl WriterError {
            pub(crate) const ALL: &[Self] = &[$(Self::$name),*];

            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$name => $wire),* }
            }
            pub(crate) const fn provider_allowed(self) -> bool {
                match self { $(Self::$name => $provider),* }
            }
            pub(crate) fn parse(value: &str) -> Option<Self> {
                Self::ALL.iter().copied().find(|code| code.as_str() == value)
            }
        }
    };
}

writer_errors! {
    ConfigurationInvalid => ("configuration_invalid", false),
    ProviderInvalid => ("provider_invalid", false),
    SourceInvalid => ("source_invalid", false),
    RuntimeUnavailable => ("runtime_unavailable", false),
    NamespaceUnavailable => ("namespace_unavailable", false),
    StorageUnavailable => ("storage_unavailable", true),
    StorageConfigurationInvalid => ("storage_configuration_invalid", false),
    PublicationInvalid => ("publication_invalid", false),
    EnvelopeNamespaceInvalid => ("envelope_namespace_invalid", false),
    CursorInvalid => ("cursor_invalid", false),
    ImmutableObjectConflict => ("immutable_object_conflict", false),
    CursorNamespaceInvalid => ("cursor_namespace_invalid", false),
    SyncBusy => ("sync_busy", false),
    SourceRetired => ("source_retired", false),
    SyncLeaseLost => ("sync_lease_lost", false),
    RejectionGenerationOverflow => ("rejection_generation_overflow", false),
    ContentBackpressure => ("content_backpressure", false),
    ProviderFailure => ("provider_failure", true),
    MembershipSnapshotIncomplete => ("membership_snapshot_incomplete", false),
    RevisionViewInvalid => ("revision_view_invalid", true),
    BatchInvalid => ("batch_invalid", true),
    PreparedRowsInvalid => ("prepared_rows_invalid", true),
    FragmentWriteFailed => ("fragment_write_failed", true),
    ChunkProfileMismatch => ("chunk_profile_mismatch", false),
    NamespaceSettingsFixed => ("namespace_settings_fixed", false),
    EmbeddingLocalCacheConfigurationInvalid => ("embedding_local_cache_configuration_invalid", false),
    EmbeddingLocalCacheUnavailable => ("embedding_local_cache_unavailable", true),
    EmbeddingLocalCacheInvalid => ("embedding_local_cache_invalid", true),
    LocalFilesReadersInvalid => ("local_files_readers_invalid", true),
    SourceRowLimit => ("source_row_limit", true),
    BatchRejected => ("batch_rejected", false),
    HostCallbackFailed => ("host_callback_failed", false),
    AdminCredentialInvalid => ("admin_credential_invalid", false),
    AdminCredentialsMustDiffer => ("admin_credentials_must_differ", false),
    AdminListenInvalid => ("admin_listen_invalid", false),
    AdminListenUnavailable => ("admin_listen_unavailable", false),
    AdminServiceFailed => ("admin_service_failed", false),
    AdminTlsRequired => ("admin_tls_required", false),
    ApiNamespaceHasSources => ("api_namespace_has_sources", false),
    WriteApiNeedsCommitter => ("write_api_needs_committer", false),
    AuthorizationUnavailable => ("authorization_unavailable", false),
    BatchHeaderExceedsLimit => ("batch_header_exceeds_limit", false),
    BatchRowsExceedLimit => ("batch_rows_exceed_limit", false),
    ChangedRowScanFailed => ("changed_row_scan_failed", false),
    ChangedRowsInvalid => ("changed_rows_invalid", false),
    CommitterBusy => ("committer_busy", false),
    ConfigInvalid => ("config_invalid", false),
    CredentialInvalid => ("credential_invalid", false),
    CredentialReferenceInvalid => ("credential_reference_invalid", false),
    CredentialUnavailable => ("credential_unavailable", false),
    DirectoryEndpointInvalid => ("directory_endpoint_invalid", false),
    DirectoryPullFailed => ("directory_pull_failed", false),
    DirectoryPullIncomplete => ("directory_pull_incomplete", false),
    DirectoryPullLimit => ("directory_pull_limit", false),
    DirectoryPullTimeout => ("directory_pull_timeout", false),
    DirectoryPullUnsupported => ("directory_pull_unsupported", false),
    DirectorySnapshotInvalid => ("directory_snapshot_invalid", false),
    DirectorySnapshotStale => ("directory_snapshot_stale", false),
    DirectorySnapshotUnavailable => ("directory_snapshot_unavailable", false),
    DirectoryVerifiedDomainsRequired => ("directory_verified_domains_required", false),
    DocumentStateInvalid => ("document_state_invalid", false),
    DuplicateChangedRows => ("duplicate_changed_rows", false),
    DuplicateDocumentChange => ("duplicate_document_change", false),
    EncryptedInputInvalid => ("encrypted_input_invalid", false),
    EncryptionKeyInvalid => ("encryption_key_invalid", false),
    EncryptionKeyMissing => ("encryption_key_missing", false),
    EncryptionKeyRotated => ("encryption_key_rotated", false),
    EncryptionUnavailable => ("encryption_unavailable", false),
    EnvelopeBoundsInvalid => ("envelope_bounds_invalid", false),
    EnvelopeIdInvalid => ("envelope_id_invalid", false),
    EnvelopeInvalid => ("envelope_invalid", false),
    HeadCasFailed => ("head_cas_failed", false),
    IdentityGenerationOverflow => ("identity_generation_overflow", false),
    LeaseEpochOverflow => ("lease_epoch_overflow", false),
    LeaseInvalid => ("lease_invalid", false),
    LeaseLost => ("lease_lost", false),
    ListingInvalid => ("listing_invalid", false),
    MembershipExpansionLimit => ("membership_expansion_limit", false),
    MembershipSnapshotInvalid => ("membership_snapshot_invalid", false),
    PermissionSnapshotInvalid => ("permission_snapshot_invalid", false),
    PolicyGenerationOverflow => ("policy_generation_overflow", false),
    PreparedFragmentInvalid => ("prepared_fragment_invalid", false),
    PublicationNamespaceInvalid => ("publication_namespace_invalid", false),
    PublicationUnavailable => ("publication_unavailable", false),
    ReceiptUnpublished => ("receipt_unpublished", false),
    RestoreHistoryIdentityChanged => ("restore_history_identity_changed", false),
    RestoreHistorySummaryLimit => ("restore_history_summary_limit", false),
    RestoreVerificationFailed => ("restore_verification_failed", false),
    RestoreWorkingSpaceExhausted => ("restore_working_space_exhausted", false),
    RetiredSourcesFull => ("retired_sources_full", false),
    SecretInputTooLarge => ("secret_input_too_large", false),
    SignalUnavailable => ("signal_unavailable", false),
    SourceUnavailable => ("source_unavailable", false),
    SourceUnknown => ("source_unknown", false),
    StaleCursor => ("stale_cursor", false),
    TooManyNamespaces => ("too_many_namespaces", false),
    TombstoneInvalid => ("tombstone_invalid", false),
    WorkerFailure => ("worker_failure", false),
    CleanupBranchUnavailable => ("cleanup_branch_unavailable", false),
    CompactionCommitFailed => ("compaction_commit_failed", false),
    CompactionFailed => ("compaction_failed", false),
    CompactionPlanFailed => ("compaction_plan_failed", false),
    CursorSequenceOverflow => ("cursor_sequence_overflow", false),
    DeleteStageFailed => ("delete_stage_failed", false),
    DocumentInvalid => ("document_invalid", false),
    DocumentRevisionMissing => ("document_revision_missing", false),
    ConditionalDeleteUnsupported => ("conditional_delete_unsupported", false),
    ExportInvalid => ("export_invalid", false),
    DeleteConfirmationInvalid => ("delete_confirmation_invalid", false),
    NamespaceDeleting => ("namespace_deleting", false),
    FileInventoryUnavailable => ("file_inventory_unavailable", false),
    ForeignLineage => ("foreign_lineage", false),
    IncompatibleIndex => ("incompatible_index", false),
    IndexBuildFailed => ("index_build_failed", false),
    IndexMergeFailed => ("index_merge_failed", false),
    IndexMergeNoProgress => ("index_merge_no_progress", false),
    LineageInvalid => ("lineage_invalid", false),
    MaintenanceStepDeadline => ("maintenance_step_deadline", false),
    NativeUpdateFailed => ("native_update_failed", false),
    PreparedSchemaInvalid => ("prepared_schema_invalid", false),
    PublicationIdInvalid => ("publication_id_invalid", false),
    PublicationSequenceOverflow => ("publication_sequence_overflow", false),
    ReadersIndexMissing => ("readers_index_missing", false),
    RestoreAuthorizationChanged => ("restore_authorization_changed", false),
    RestoreCommitFailed => ("restore_commit_failed", false),
    RestoreHorizonExpired => ("restore_horizon_expired", false),
    TransactionInvalid => ("transaction_invalid", false),
    VerificationFailed => ("verification_failed", false),
    VersionCleanupFailed => ("version_cleanup_failed", false),
    EmbeddingConfigurationInvalid => ("embedding_configuration_invalid", false),
    EmbeddingSpendLimitRequired => ("embedding_spend_limit_required", false),
    EmbeddingSpendConfigurationInvalid => ("embedding_spend_configuration_invalid", false),
    EmbeddingSpendLimitExceeded => ("embedding_spend_limit_exceeded", false),
    EmbeddingCredentialsUnavailable => ("embedding_credentials_unavailable", false),
    EmbeddingMemoryBudgetExhausted => ("embedding_memory_budget_exhausted", false),
    EmbeddingTimeout => ("embedding_timeout", false),
    EmbeddingProviderUnavailable => ("embedding_provider_unavailable", false),
    EmbeddingResponseInvalid => ("embedding_response_invalid", false),
    EmbeddingModelMismatch => ("embedding_model_mismatch", false),
    EmbeddingBatchInvalid => ("embedding_batch_invalid", false),
    EmbeddingInvalid => ("embedding_invalid", false),
    EmbeddingProfileInvalid => ("embedding_profile_invalid", false),
    EmbeddingProfileMismatch => ("embedding_profile_mismatch", false),
    EmbeddingDimensionMismatch => ("embedding_dimension_mismatch", false),
    EmbeddingUnavailable => ("embedding_unavailable", false),
}

impl From<fyrr_reader::embedding::EmbeddingError> for WriterError {
    fn from(error: fyrr_reader::embedding::EmbeddingError) -> Self {
        use fyrr_reader::embedding::EmbeddingError as E;
        match error {
            E::EmbeddingConfigurationInvalid => Self::EmbeddingConfigurationInvalid,
            E::EmbeddingSpendLimitRequired => Self::EmbeddingSpendLimitRequired,
            E::EmbeddingSpendConfigurationInvalid => Self::EmbeddingSpendConfigurationInvalid,
            E::EmbeddingSpendLimitExceeded => Self::EmbeddingSpendLimitExceeded,
            E::EmbeddingCredentialsUnavailable => Self::EmbeddingCredentialsUnavailable,
            E::EmbeddingMemoryBudgetExhausted => Self::EmbeddingMemoryBudgetExhausted,
            E::EmbeddingTimeout => Self::EmbeddingTimeout,
            E::EmbeddingProviderUnavailable => Self::EmbeddingProviderUnavailable,
            E::EmbeddingResponseInvalid => Self::EmbeddingResponseInvalid,
            E::EmbeddingModelMismatch => Self::EmbeddingModelMismatch,
            E::EmbeddingBatchInvalid => Self::EmbeddingBatchInvalid,
            E::EmbeddingInvalid => Self::EmbeddingInvalid,
            E::EmbeddingProfileInvalid => Self::EmbeddingProfileInvalid,
            E::EmbeddingProfileMismatch => Self::EmbeddingProfileMismatch,
            E::EmbeddingDimensionMismatch => Self::EmbeddingDimensionMismatch,
            E::EmbeddingUnavailable => Self::EmbeddingUnavailable,
        }
    }
}

impl From<fyrr_reader::IndexVerificationError> for WriterError {
    fn from(error: fyrr_reader::IndexVerificationError) -> Self {
        use fyrr_reader::IndexVerificationError as I;
        match error {
            I::StorageUnavailable => Self::StorageUnavailable,
            I::ReadersIndexMissing => Self::ReadersIndexMissing,
            I::IncompatibleIndex => Self::IncompatibleIndex,
        }
    }
}

impl From<fyrr_reader::storage::StorageError> for WriterError {
    fn from(error: fyrr_reader::storage::StorageError) -> Self {
        use fyrr_reader::storage::StorageError as S;
        match error {
            S::StorageConfigurationInvalid => Self::StorageConfigurationInvalid,
            S::StorageUnavailable => Self::StorageUnavailable,
            S::PublicationInvalid => Self::PublicationInvalid,
            S::AuthorizationUnavailable => Self::AuthorizationUnavailable,
            S::StaleCursor => Self::StaleCursor,
            S::PublicationUnavailable => Self::PublicationUnavailable,
            S::TooManyNamespaces => Self::TooManyNamespaces,
        }
    }
}

impl std::fmt::Display for WriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl std::error::Error for WriterError {}
impl serde::Serialize for WriterError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(test)]
mod tests;

impl From<WriterError> for String {
    fn from(code: WriterError) -> Self {
        code.as_str().to_owned()
    }
}

impl From<object_store::Error> for WriterError {
    fn from(_: object_store::Error) -> Self {
        Self::StorageUnavailable
    }
}
impl From<lance::Error> for WriterError {
    fn from(_: lance::Error) -> Self {
        Self::StorageUnavailable
    }
}

// Provider text cannot select a host-only diagnostic.
impl WriterError {
    pub(crate) fn from_provider(code: String) -> Self {
        Self::parse(&code)
            .filter(|code| code.provider_allowed())
            .unwrap_or(Self::BatchRejected)
    }
}
