use super::WriterError;
use fyrr_reader::storage::StorageError as S;

#[test]
fn writer_error_wire_strings_and_admission_are_unchanged() {
    // Independent literals pin the existing wire contract and provider boundary.
    let expected = [
        ("configuration_invalid", false),
        ("provider_invalid", false),
        ("source_invalid", false),
        ("runtime_unavailable", false),
        ("namespace_unavailable", false),
        ("storage_unavailable", true),
        ("storage_configuration_invalid", false),
        ("publication_invalid", false),
        ("envelope_namespace_invalid", false),
        ("cursor_invalid", false),
        ("immutable_object_conflict", false),
        ("cursor_namespace_invalid", false),
        ("sync_busy", false),
        ("source_retired", false),
        ("sync_lease_lost", false),
        ("rejection_generation_overflow", false),
        ("content_backpressure", false),
        ("provider_failure", true),
        ("membership_snapshot_incomplete", false),
        ("revision_view_invalid", true),
        ("batch_invalid", true),
        ("prepared_rows_invalid", true),
        ("fragment_write_failed", true),
        ("chunk_profile_mismatch", false),
        ("namespace_settings_fixed", false),
        ("attribute_type_change_refused", false),
        ("attribute_removal_refused", false),
        ("embedding_local_cache_configuration_invalid", false),
        ("embedding_local_cache_unavailable", true),
        ("embedding_local_cache_invalid", true),
        ("local_files_readers_invalid", true),
        ("source_row_limit", true),
        ("batch_rejected", false),
        ("host_callback_failed", false),
        ("admin_credential_invalid", false),
        ("admin_credentials_must_differ", false),
        ("admin_listen_invalid", false),
        ("admin_listen_unavailable", false),
        ("admin_service_failed", false),
        ("admin_tls_required", false),
        ("api_namespace_has_sources", false),
        ("write_api_needs_committer", false),
        ("authorization_unavailable", false),
        ("batch_header_exceeds_limit", false),
        ("batch_rows_exceed_limit", false),
        ("changed_row_scan_failed", false),
        ("changed_rows_invalid", false),
        ("committer_busy", false),
        ("config_invalid", false),
        ("credential_invalid", false),
        ("credential_reference_invalid", false),
        ("credential_unavailable", false),
        ("directory_endpoint_invalid", false),
        ("directory_pull_failed", false),
        ("directory_pull_incomplete", false),
        ("directory_pull_limit", false),
        ("directory_pull_timeout", false),
        ("directory_pull_unsupported", false),
        ("directory_snapshot_invalid", false),
        ("directory_snapshot_stale", false),
        ("directory_snapshot_unavailable", false),
        ("directory_verified_domains_required", false),
        ("document_state_invalid", false),
        ("duplicate_changed_rows", false),
        ("duplicate_document_change", false),
        ("encrypted_input_invalid", false),
        ("encryption_key_invalid", false),
        ("encryption_key_missing", false),
        ("encryption_key_rotated", false),
        ("encryption_unavailable", false),
        ("envelope_bounds_invalid", false),
        ("envelope_id_invalid", false),
        ("envelope_invalid", false),
        ("head_cas_failed", false),
        ("identity_generation_overflow", false),
        ("lease_epoch_overflow", false),
        ("lease_invalid", false),
        ("lease_lost", false),
        ("listing_invalid", false),
        ("membership_expansion_limit", false),
        ("membership_snapshot_invalid", false),
        ("permission_snapshot_invalid", false),
        ("policy_generation_overflow", false),
        ("prepared_fragment_invalid", false),
        ("publication_namespace_invalid", false),
        ("publication_unavailable", false),
        ("receipt_unpublished", false),
        ("restore_history_identity_changed", false),
        ("restore_history_summary_limit", false),
        ("restore_verification_failed", false),
        ("restore_working_space_exhausted", false),
        ("retired_sources_full", false),
        ("secret_input_too_large", false),
        ("signal_unavailable", false),
        ("source_unavailable", false),
        ("source_unknown", false),
        ("stale_cursor", false),
        ("too_many_namespaces", false),
        ("tombstone_invalid", false),
        ("worker_failure", false),
        ("cleanup_branch_unavailable", false),
        ("compaction_commit_failed", false),
        ("compaction_failed", false),
        ("compaction_plan_failed", false),
        ("cursor_sequence_overflow", false),
        ("delete_stage_failed", false),
        ("document_invalid", false),
        ("document_revision_missing", false),
        ("conditional_delete_unsupported", false),
        ("export_invalid", false),
        ("delete_confirmation_invalid", false),
        ("namespace_deleting", false),
        ("file_inventory_unavailable", false),
        ("foreign_lineage", false),
        ("incompatible_index", false),
        ("index_build_failed", false),
        ("index_merge_failed", false),
        ("index_merge_no_progress", false),
        ("lineage_invalid", false),
        ("maintenance_step_deadline", false),
        ("native_update_failed", false),
        ("prepared_schema_invalid", false),
        ("publication_id_invalid", false),
        ("publication_sequence_overflow", false),
        ("readers_index_missing", false),
        ("restore_authorization_changed", false),
        ("restore_commit_failed", false),
        ("restore_horizon_expired", false),
        ("transaction_invalid", false),
        ("verification_failed", false),
        ("version_cleanup_failed", false),
        ("embedding_configuration_invalid", false),
        ("embedding_spend_limit_required", false),
        ("embedding_spend_configuration_invalid", false),
        ("embedding_spend_limit_exceeded", false),
        ("embedding_credentials_unavailable", false),
        ("embedding_memory_budget_exhausted", false),
        ("embedding_timeout", false),
        ("embedding_provider_unavailable", false),
        ("embedding_response_invalid", false),
        ("embedding_model_mismatch", false),
        ("embedding_batch_invalid", false),
        ("embedding_invalid", false),
        ("embedding_profile_invalid", false),
        ("embedding_profile_mismatch", false),
        ("embedding_dimension_mismatch", false),
        ("embedding_unavailable", false),
    ];
    assert_eq!(WriterError::ALL.len(), expected.len());
    for (code, (wire, provider)) in WriterError::ALL.iter().copied().zip(expected) {
        assert_eq!(code.as_str(), wire);
        assert_eq!(WriterError::parse(wire), Some(code));
        assert_eq!(code.provider_allowed(), provider);
        assert_eq!(code.to_string(), wire);
        assert_eq!(serde_json::to_value(code).unwrap(), serde_json::json!(wire));
        assert_eq!(
            WriterError::from_provider(wire.to_owned()).as_str(),
            if provider { wire } else { "batch_rejected" }
        );
    }
    for &error in fyrr_reader::embedding::EmbeddingError::ALL {
        let mapped = WriterError::from(error);
        assert_eq!(mapped.as_str(), error.as_str());
        assert_ne!(mapped, WriterError::HostCallbackFailed);
    }
    assert!(WriterError::parse("unregistered").is_none());
    assert_eq!(
        WriterError::from_provider("unregistered".to_owned()).as_str(),
        "batch_rejected"
    );
    for (storage, writer, wire) in [
        (
            S::StorageConfigurationInvalid,
            WriterError::StorageConfigurationInvalid,
            "storage_configuration_invalid",
        ),
        (
            S::StorageUnavailable,
            WriterError::StorageUnavailable,
            "storage_unavailable",
        ),
        (
            S::PublicationInvalid,
            WriterError::PublicationInvalid,
            "publication_invalid",
        ),
        (
            S::AuthorizationUnavailable,
            WriterError::AuthorizationUnavailable,
            "authorization_unavailable",
        ),
        (S::StaleCursor, WriterError::StaleCursor, "stale_cursor"),
        (
            S::TooManyNamespaces,
            WriterError::TooManyNamespaces,
            "too_many_namespaces",
        ),
        (
            S::PublicationUnavailable,
            WriterError::PublicationUnavailable,
            "publication_unavailable",
        ),
    ] {
        assert_eq!(WriterError::from(storage), writer);
        assert_eq!(writer.as_str(), wire);
    }
    for (index, writer, wire) in [
        (
            fyrr_reader::IndexVerificationError::StorageUnavailable,
            WriterError::StorageUnavailable,
            "storage_unavailable",
        ),
        (
            fyrr_reader::IndexVerificationError::ReadersIndexMissing,
            WriterError::ReadersIndexMissing,
            "readers_index_missing",
        ),
        (
            fyrr_reader::IndexVerificationError::IncompatibleIndex,
            WriterError::IncompatibleIndex,
            "incompatible_index",
        ),
    ] {
        assert_eq!(WriterError::from(index), writer);
        assert_eq!(writer.as_str(), wire);
    }
}
