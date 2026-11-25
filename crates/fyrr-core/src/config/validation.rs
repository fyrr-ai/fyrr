use super::*;

impl LoadedConfig {
    /// Cross-field validation. Errors make the file unusable.
    pub fn validate(&self) -> Vec<Issue> {
        let config = &self.config;
        let mut issues = Vec::new();
        let mut error = |path: &str, message: String| issues.push(Issue::error(path, message));
        rerank(config, &mut error);
        identity_keys(config, &mut error);
        api_key::validate(config, &mut error);
        identity_urls_and_http(config, &mut error);
        patterns(config, &mut error);
        namespace_settings(&config.namespace, config.embedding.dimensions, &mut error);
        embedding(config, &mut error);
        ranges(config, &mut error);
        storage(config, &mut error);
        storage::local(config, &self.raw, &mut error);
        embedding_endpoint(config, &mut error);
        reader(config, &mut error);
        writer_bounds(config, &mut error);
        positive_intervals(config, &mut error);
        administration(config, &mut error);
        metrics(config, &mut error);
        directory(config, &mut error);
        writer_credentials(config, &mut error);
        sources(config, &mut error);
        if config.namespace.write_api.is_some() && !config.sources.is_empty() {
            error(
                "namespace.write_api",
                "a namespace written through the API takes no [[sources]]".into(),
            );
        }
        for (index, source) in config.sources.iter().enumerate() {
            if source.permissions == PermissionsMode::Fixed {
                issues.push(Issue::warning(
                    format!("sources[{index}].permissions"),
                    "fixed connection: every listed reader may read every document; the operator owns the over-sharing risk",
                ));
            }
        }
        issues
    }
}

fn rerank(config: &Config, error: &mut impl FnMut(&str, String)) {
    if let Some(rerank) = &config.namespace.rerank {
        if secure_url(&rerank.endpoint).is_none() || rerank.endpoint.len() as u64 > URL_MAX_BYTES {
            error(
                "namespace.rerank.endpoint",
                "requires HTTPS or loopback HTTP without credentials, query or fragment".into(),
            );
        }
        rerank_settings(&rerank.settings(), error);
        if !(std::time::Duration::from_millis(1)..=std::time::Duration::from_secs(300))
            .contains(&rerank.deadline.as_duration())
        {
            error(
                "namespace.rerank.deadline",
                "must be between 1ms and 300s".into(),
            );
        }
        if rerank.max_call_cost_microusd == 0 {
            error(
                "namespace.rerank.max_call_cost_microusd",
                "must be positive".into(),
            );
        }
    }
}

/// What a namespace records of its reranker.
pub(crate) fn rerank_settings(rerank: &RerankSettings, error: &mut impl FnMut(&str, String)) {
    if rerank.model.trim().is_empty() || rerank.model.len() > 256 {
        error("namespace.rerank.model", "must contain 1–256 bytes".into());
    }
    if let Some(message) = range_issue(rerank.candidates as u64, 1, RESULT_SLOTS_MAX) {
        error("namespace.rerank.candidates", message);
    }
    if !rerank.weight.is_finite() || !(0.0..=1.0).contains(&rerank.weight) {
        error("namespace.rerank.weight", "must be between 0 and 1".into());
    }
    if let Some(chars) = rerank.text_chars
        && let Some(message) =
            range_issue(chars as u64, RERANK_TEXT_CHARS_MIN, RERANK_TEXT_CHARS_MAX)
    {
        error("namespace.rerank.text_chars", message);
    }
}

fn identity_keys(config: &Config, error: &mut impl FnMut(&str, String)) {
    let identity = &config.identity;
    if identity.hashing_key.is_none() {
        error(
            "identity.hashing_key",
            "required: reader tokens and subjects are stored only as HMACs under this key".into(),
        );
    }
    if identity.encryption_key.is_some() {
        return;
    }
    if let Some(index) = config
        .sources
        .iter()
        .position(|source| source.permissions == PermissionsMode::Sync)
    {
        error(
            "identity.encryption_key",
            format!(
                "required by sources[{index}]: permissions = \"sync\" stores encrypted member snapshots"
            ),
        );
    }
    if identity.directory.provider == DirectoryProvider::Snapshot {
        error(
            "identity.encryption_key",
            "required for directory snapshots, which are stored encrypted".into(),
        );
    }
}

fn identity_urls_and_http(config: &Config, error: &mut impl FnMut(&str, String)) {
    if let Some(oidc) = &config.identity.oidc {
        if oidc.issuer.is_empty() {
            error(
                "identity.oidc.issuer",
                "required unless identity.directory.provider = \"keycloak\" supplies it".into(),
            );
        }
        if oidc.jwks_url.is_empty() {
            error(
                "identity.oidc.jwks_url",
                "required unless keycloak = true derives it from the issuer".into(),
            );
        }
        if [&oidc.issuer, &oidc.jwks_url]
            .iter()
            .any(|value| !value.is_empty() && secure_url(value).is_none())
        {
            error(
                "identity.oidc",
                "requires HTTPS or loopback HTTP URLs without credentials, query or fragment"
                    .into(),
            );
        }
        if let Some(directory) = config.identity.directory.subject_issuer()
            && !oidc.issuer.is_empty()
            && oidc.issuer != directory
        {
            error(
                "identity.oidc.issuer",
                "differs from the directory issuer (identity.directory.issuer, or <endpoint>/realms/<realm> for Keycloak), which binds every subject: no token would be admitted".into(),
            );
        }
    }
    if let Some(endpoint) = &config.identity.directory.endpoint
        && secure_url(endpoint).is_none()
    {
        error(
            "identity.directory.endpoint",
            "requires HTTPS or loopback HTTP URL without credentials, query or fragment".into(),
        );
    }
    if let Some(http) = &config.reader.http {
        if secure_url(&http.public_url).is_none_or(|url| url.path() != "/mcp") {
            error(
                "reader.http.public_url",
                "requires a secure /mcp URL".into(),
            );
        }
        if http.listen.parse::<std::net::SocketAddr>().is_err()
            || http.public_url.is_empty()
            || http.public_url.len() as u64 > URL_MAX_BYTES
            || http.authorization_cache_subjects == 0
            || http.authorization_cache_subjects as u64 > AUTHORIZATION_CACHE_SUBJECTS_MAX
            || http.user_requests_per_minute == 0
            || http.group_requests_per_minute == 0
        {
            error("reader.http", "invalid bounded HTTP configuration".into());
        }
        if let Some(oidc) = &config.identity.oidc
            && (oidc.jwks_url.is_empty()
                || oidc.jwks_url.len() as u64 > URL_MAX_BYTES
                || oidc.scope.is_empty()
                || oidc.scope.len() > 256
                || !oidc
                    .scope
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b":._-/".contains(&b))
                || oidc.issuer.len() as u64 > URL_MAX_BYTES
                || oidc.audience.is_empty()
                || oidc.audience.len() as u64 > URL_MAX_BYTES
                || oidc.jwks_validity.as_duration() < std::time::Duration::from_secs(1)
                || oidc.jwks_validity.as_duration() > std::time::Duration::from_secs(300)
                || oidc.jwks_refresh_interval.as_duration() < std::time::Duration::from_millis(100))
        {
            error(
                "identity.oidc",
                "HTTP requires pinned JWKS, audience, scope and issuer-scoped subject".into(),
            );
        }
    }
}

fn patterns(config: &Config, error: &mut impl FnMut(&str, String)) {
    // The schema's patterns and ranges (same constants as the attributes).
    let mut patterns = vec![
        (
            "namespace.name".to_owned(),
            config.namespace.name.as_str(),
            &*NAMESPACE,
        ),
        (
            "storage.uri".to_owned(),
            config.storage.uri.as_str(),
            &*STORAGE_URI,
        ),
    ];
    if let Some(key) = &config.storage.kms_key_id {
        patterns.push(("storage.kms_key_id".to_owned(), key.as_str(), &*KMS_KEY_ID));
    }
    for (index, source) in config.sources.iter().enumerate() {
        patterns.push((
            format!("sources[{index}].id"),
            source.id.as_str(),
            &*SOURCE_ID,
        ));
        patterns.push((
            format!("sources[{index}].type"),
            source.provider.as_str(),
            &*PROVIDER,
        ));
    }
    for (path, value, pattern) in patterns {
        if !pattern.is_match(value) {
            error(
                &path,
                format!("does not match the pattern {}", pattern.as_str()),
            );
        }
    }
}

/// What a namespace is created with: the same bounds for a configuration file
/// and for the settings a policy artifact records.
pub(crate) fn namespace_settings(
    namespace: &NamespaceConfig,
    embedding_dimensions: u32,
    error: &mut impl FnMut(&str, String),
) {
    dense_weight(namespace, error);
    rescore_depth(namespace, error);
    composition(namespace, error);
    namespace_ranges(namespace, error);
    if let Some(analyzer) = &namespace.analyzer {
        if !analyzer.is_valid() {
            error(
                "namespace.analyzer",
                "a setting is out of its bounds, or is one its tokenizer ignores".into(),
            );
        } else if analyzer.tokenizer == crate::attributes::analyzer::Tokenizer::Ngram {
            error(
                "namespace.analyzer",
                "the ngram tokenizer is not offered for the text: attributes inherit it, and a phrase or an `all` match would find rows with any one n-gram".into(),
            );
        } else if namespace.fts_stem
            || namespace.fts_remove_stop_words
            || namespace.fts_ascii_folding
            || namespace.fts_max_token_length != super::default_fts_max_token_length()
        {
            error(
                "namespace.analyzer",
                "replaces fts_stem, fts_remove_stop_words, fts_ascii_folding and fts_max_token_length: leave them out".into(),
            );
        }
    }
    if let Err(code) = crate::attributes::check_schema(&namespace.attributes) {
        error("namespace.attributes", code.to_string());
    } else if let Err(code) =
        crate::attributes::check_indexes(&namespace.attributes, &namespace.attribute_indexes)
    {
        error("namespace.attribute_indexes", code.to_string());
    }
    if !namespace.vectors.is_empty() {
        if namespace.write_api.is_none() {
            error(
                "namespace.vectors",
                "vector attributes are written through the write API".into(),
            );
        } else if let Err(code) = crate::attributes::check_vectors(
            &namespace.attributes,
            &namespace.vectors,
            embedding_dimensions,
            namespace.dense_metric,
        ) {
            error("namespace.vectors", code.to_string());
        }
    }
    if namespace.write_api.is_none() {
        for (name, kind) in crate::attributes::CONNECTOR_COLUMNS {
            if namespace.attributes.get(name) != Some(&kind) {
                error(
                    "namespace.attributes",
                    format!("the connector column {name} is missing or has another type"),
                );
            }
        }
    }
    if let Some(api) = &namespace.write_api {
        if let Err(code) = crate::attributes::check_name(&api.text_attribute) {
            error("namespace.write_api.text_attribute", code.to_string());
        } else if namespace.attributes.contains_key(&api.text_attribute)
            || namespace.vectors.contains_key(&api.text_attribute)
        {
            error(
                "namespace.write_api.text_attribute",
                "is also declared in namespace.attributes or namespace.vectors".into(),
            );
        }
    }
}

fn dense_weight(namespace: &NamespaceConfig, error: &mut impl FnMut(&str, String)) {
    if !namespace.retrieval.dense_weight.is_finite()
        || !(0.0..=1.0).contains(&namespace.retrieval.dense_weight)
    {
        error(
            "namespace.retrieval.dense_weight",
            "must be between 0 and 1".into(),
        );
    }
}

fn embedding(config: &Config, error: &mut impl FnMut(&str, String)) {
    if config.embedding.timeout.as_duration().is_zero() {
        error(
            "embedding.timeout",
            "embedding_configuration_invalid".into(),
        );
    }
    if config.embedding.reader_live && config.embedding.spend.is_none() {
        error("embedding.spend", "embedding_spend_limit_required".into());
    }
    if let Some(spend) = &config.embedding.spend
        && (spend.limit_microusd == 0
            || spend.max_call_microusd == 0
            || spend.max_call_microusd > spend.limit_microusd)
    {
        error(
            "embedding.spend",
            "embedding_spend_configuration_invalid".into(),
        );
    }
    for (role, transform) in [
        ("document", &config.embedding.roles.document),
        ("query", &config.embedding.roles.query),
    ] {
        if transform
            .prefix
            .as_ref()
            .is_some_and(|prefix| prefix.len() as u64 > PREFIX_MAX_BYTES)
            || transform.input_type.as_ref().is_some_and(|kind| {
                kind.is_empty()
                    || kind.len() as u64 > INPUT_TYPE_MAX_BYTES
                    || kind.chars().any(char::is_control)
            })
        {
            error(
                &format!("embedding.roles.{role}"),
                "invalid role transformation".into(),
            );
        }
    }
    if config.embedding.model.is_empty() || config.embedding.model.len() as u64 > MODEL_MAX_BYTES {
        error(
            "embedding.model",
            format!("must contain 1–{MODEL_MAX_BYTES} UTF-8 bytes"),
        );
    }
    if config.embedding.prefix.len() as u64 > PREFIX_MAX_BYTES {
        error(
            "embedding.prefix",
            format!("must contain at most {PREFIX_MAX_BYTES} UTF-8 bytes"),
        );
    }
}

fn rescore_depth(namespace: &NamespaceConfig, error: &mut impl FnMut(&str, String)) {
    match &namespace.retrieval.rescore_depth {
        Some(RescoreDepth::Fixed { candidates }) if !(1..=1200).contains(candidates) => error(
            "namespace.retrieval.rescore_depth",
            "candidates must be in 1–1200".into(),
        ),
        Some(RescoreDepth::Adaptive {
            min_candidates,
            max_candidates,
            min_gap,
        }) if !(1..=1200).contains(min_candidates)
            || !(1..=1200).contains(max_candidates)
            || min_candidates > max_candidates
            || !min_gap.is_finite()
            || *min_gap < 0.0 =>
        {
            error(
                "namespace.retrieval.rescore_depth",
                "invalid adaptive bounds or gap".into(),
            );
        }
        _ => {}
    }
}

fn namespace_ranges(namespace: &NamespaceConfig, error: &mut impl FnMut(&str, String)) {
    let ranges = [
        (
            "namespace.retrieval.dense_candidates".into(),
            namespace.retrieval.dense_candidates as u64,
            1,
            RESULT_SLOTS_MAX,
        ),
        (
            "namespace.retrieval.lexical_candidates".into(),
            namespace.retrieval.lexical_candidates as u64,
            1,
            LEXICAL_CANDIDATES_MAX,
        ),
        (
            "namespace.retrieval.source_cap".into(),
            namespace.retrieval.source_cap as u64,
            0,
            RESULT_SLOTS_MAX,
        ),
        (
            "namespace.retrieval.max_documents".into(),
            namespace.retrieval.max_documents as u64,
            1,
            MAX_DOCUMENTS_MAX,
        ),
        (
            "namespace.fts_max_token_length".to_owned(),
            u64::from(namespace.fts_max_token_length),
            1,
            FTS_MAX_TOKEN_LENGTH_MAX,
        ),
        (
            "namespace.dense_rq_bits".to_owned(),
            u64::from(namespace.dense_rq_bits),
            1,
            DENSE_RQ_BITS_MAX,
        ),
        (
            "namespace.dense_partition_rows".to_owned(),
            u64::from(namespace.dense_partition_rows),
            DENSE_PARTITION_ROWS_MIN,
            DENSE_PARTITION_ROWS_MAX,
        ),
    ];
    for (path, value, min, max) in ranges {
        if let Some(message) = range_issue(value, min, max) {
            error(&path, message);
        }
    }
}

fn ranges(config: &Config, error: &mut impl FnMut(&str, String)) {
    let mut ranges = vec![
        (
            "embedding.query_cache_max_bytes".into(),
            config.embedding.query_cache_max_bytes,
            0,
            QUERY_CACHE_MAX_BYTES,
        ),
        (
            "embedding.dimensions".to_owned(),
            u64::from(config.embedding.dimensions),
            1,
            DIMENSIONS_MAX,
        ),
        (
            "embedding.batch_size".to_owned(),
            u64::from(config.embedding.batch_size),
            1,
            BATCH_SIZE_MAX,
        ),
        (
            "embedding.max_retries".to_owned(),
            u64::from(config.embedding.max_retries),
            0,
            MAX_RETRIES_MAX,
        ),
        (
            "identity.max_tokens_per_user".to_owned(),
            u64::from(config.identity.max_tokens_per_user),
            1,
            u64::MAX,
        ),
        (
            "writer.envelope_max_rows".to_owned(),
            config.writer.envelope_max_rows,
            1,
            u64::MAX,
        ),
        (
            "writer.commit.group_max_rows".to_owned(),
            config.writer.commit.group_max_rows,
            1,
            u64::MAX,
        ),
        (
            "writer.backpressure.max_pending_content_envelopes".to_owned(),
            config.writer.backpressure.max_pending_content_envelopes,
            1,
            u64::MAX,
        ),
    ];
    for (index, source) in config.sources.iter().enumerate() {
        if let Some(days) = source.retention_days {
            ranges.push((
                format!("sources[{index}].retention_days"),
                u64::from(days),
                1,
                u64::MAX,
            ));
        }
    }
    for (path, value, min, max) in ranges {
        if let Some(message) = range_issue(value, min, max) {
            error(&path, message);
        }
    }
}

fn storage(config: &Config, error: &mut impl FnMut(&str, String)) {
    let storage = &config.storage;
    if let Some(endpoint) = &storage.endpoint {
        if endpoint.starts_with("http://") {
            if !storage.allow_http {
                error(
                    "storage.endpoint",
                    "a plain-HTTP endpoint needs storage.allow_http = true".into(),
                );
            }
        } else if !endpoint.starts_with("https://") {
            error(
                "storage.endpoint",
                "must be an http:// or https:// URL".into(),
            );
        }
    }
    if storage.access_key_id.is_some() != storage.secret_access_key.is_some() {
        error(
            "storage.access_key_id",
            "set access_key_id and secret_access_key together, or neither".into(),
        );
    }
    if storage.session_token.is_some() && storage.access_key_id.is_none() {
        error(
            "storage.session_token",
            "needs access_key_id and secret_access_key".into(),
        );
    }
}

fn embedding_endpoint(config: &Config, error: &mut impl FnMut(&str, String)) {
    let endpoint = &config.embedding.endpoint;
    if !endpoint.is_empty()
        && !(endpoint.starts_with("https://") || endpoint.starts_with("http://"))
    {
        error(
            "embedding.endpoint",
            "must be an http:// or https:// URL".into(),
        );
    }
}

fn reader(config: &Config, error: &mut impl FnMut(&str, String)) {
    let reader = &config.reader;
    for (path, value, max) in [
        ("reader.concurrency", reader.concurrency, CONCURRENCY_MAX),
        (
            "reader.filter_count_max_rows",
            reader.filter_count_max_rows,
            FILTER_COUNT_MAX_ROWS,
        ),
        ("reader.nprobes", reader.nprobes, NPROBES_MAX),
    ] {
        if let Some(message) = range_issue(value as u64, 1, max) {
            error(path, message);
        }
    }
    if let Some(chunk) = reader.fts_search_chunk
        && let Some(message) = range_issue(chunk as u64, 1, FTS_SEARCH_CHUNK_MAX)
    {
        error("reader.fts_search_chunk", message);
    }
    if reader.mirror_path.as_deref().is_some_and(str::is_empty) {
        error(
            "reader.mirror_path",
            "must name a directory; leave it out for the default".into(),
        );
    }
    if let Some(bytes) = reader.max_response_bytes
        && let Some(message) = range_issue(bytes as u64, RESPONSE_BYTES_MIN, RESPONSE_BYTES_MAX)
    {
        error("reader.max_response_bytes", message);
    }
    if reader
        .instructions
        .as_ref()
        .is_some_and(|text| text.chars().count() as u64 > INSTRUCTIONS_MAX_CHARS)
    {
        error(
            "reader.instructions",
            format!("must contain at most {INSTRUCTIONS_MAX_CHARS} characters"),
        );
    }
    if config.reader.memory_max_bytes.as_u64() == 0
        || config.reader.memory_headroom_bytes >= config.reader.memory_max_bytes
    {
        error(
            "reader.memory_headroom_bytes",
            "must be smaller than nonzero reader.memory_max_bytes".into(),
        );
    }
}

fn composition(namespace: &NamespaceConfig, error: &mut impl FnMut(&str, String)) {
    let composition = &namespace.composition;
    if !(1..=RESULT_SLOTS_MAX).contains(&(composition.fusion as u64))
        || composition.dense as u64 >= RESULT_SLOTS_MAX
        || composition.lexical as u64 >= RESULT_SLOTS_MAX
        || composition
            .fusion
            .saturating_add(composition.dense)
            .saturating_add(composition.lexical) as u64
            > RESULT_SLOTS_MAX
    {
        error(
            "namespace.composition",
            format!("slots must total at most {RESULT_SLOTS_MAX} with at least one fusion slot"),
        );
    }
}

fn writer_bounds(config: &Config, error: &mut impl FnMut(&str, String)) {
    let writer = &config.writer;
    if writer.purge_window <= writer.version_grace {
        error(
            "writer.purge_window",
            format!(
                "must exceed writer.version_grace ({} <= {})",
                writer.purge_window, writer.version_grace
            ),
        );
    }
    let grace_floor = MAX_REQUEST_DEADLINE.saturating_add(config.reader.max_preparation);
    if writer.version_grace <= grace_floor {
        error(
            "writer.version_grace",
            format!(
                "must exceed the maximum request deadline ({MAX_REQUEST_DEADLINE}) plus reader.max_preparation ({}), i.e. {grace_floor}",
                config.reader.max_preparation
            ),
        );
    }
    let envelope_age_limit = ConfigDuration::from_millis(
        LANCE_UNREFERENCED_RETENTION.as_millis() - ENVELOPE_AGE_MARGIN.as_millis(),
    );
    if writer.max_envelope_age > envelope_age_limit || writer.max_envelope_age.as_millis() == 0 {
        error(
            "writer.max_envelope_age",
            format!(
                "must be between 1ms and {envelope_age_limit} (Lance keeps unreferenced fragments for {LANCE_UNREFERENCED_RETENTION}, minus a {ENVELOPE_AGE_MARGIN} margin)"
            ),
        );
    }
    if writer.threads == Some(0) {
        error("writer.threads", "must be at least 1".into());
    }
    if let Some(megabytes) = writer.fts_partition_memory_mb
        && let Some(message) = range_issue(u64::from(megabytes), 1, FTS_PARTITION_MEMORY_MB_MAX)
    {
        error("writer.fts_partition_memory_mb", message);
    }
    if writer.worker_memory_max_bytes.as_u64() == 0
        || writer.worker_memory_max_bytes.as_u64() > i64::MAX as u64
    {
        error(
            "writer.worker_memory_max_bytes",
            "must be between 1B and 9223372036854775807B".into(),
        );
    }
    if writer.envelope_max_bytes <= writer.document_max_bytes {
        error(
            "writer.envelope_max_bytes",
            "must exceed writer.document_max_bytes: a document never spans envelopes".into(),
        );
    }
    if writer.commit.group_max_bytes < writer.envelope_max_bytes
        || writer.commit.group_max_rows < writer.envelope_max_rows
    {
        error(
            "writer.commit",
            "group_max_bytes and group_max_rows must hold at least one full envelope".into(),
        );
    }
}

fn positive_intervals(config: &Config, error: &mut impl FnMut(&str, String)) {
    let writer = &config.writer;
    for (path, value) in [
        (
            "writer.commit.coalescing_timer",
            writer.commit.coalescing_timer,
        ),
        ("writer.commit.lease_ttl", writer.commit.lease_ttl),
        (
            "writer.maintenance.step_max_duration",
            writer.maintenance.step_max_duration,
        ),
        (
            "reader.version_check_interval",
            config.reader.version_check_interval,
        ),
        ("reader.max_preparation", config.reader.max_preparation),
        (
            "identity.directory.sync_interval",
            config.identity.directory.sync_interval,
        ),
    ] {
        if value.as_millis() == 0 {
            error(path, "must be greater than zero".into());
        }
    }
}

fn metrics(config: &Config, error: &mut impl FnMut(&str, String)) {
    for (path, address) in [
        (
            "reader.http.metrics_listen",
            config
                .reader
                .http
                .as_ref()
                .and_then(|http| http.metrics_listen.as_deref()),
        ),
        (
            "writer.serve.metrics_listen",
            config.writer.serve.metrics_listen.as_deref(),
        ),
    ] {
        if address.is_some_and(|address| address.parse::<std::net::SocketAddr>().is_err()) {
            error(path, "must be an IP socket address".into());
        }
    }
}

fn administration(config: &Config, error: &mut impl FnMut(&str, String)) {
    let writer = &config.writer;
    if let Some(admin) = &writer.serve.admin {
        match admin.listen.parse::<std::net::SocketAddr>() {
            Ok(address) if address.ip().is_loopback() || admin.tls_terminated => {}
            Ok(_) => error(
                "writer.serve.admin.tls_terminated",
                "a non-loopback listener requires a TLS-terminating proxy".into(),
            ),
            Err(_) => error(
                "writer.serve.admin.listen",
                "must be an IP socket address".into(),
            ),
        }
        if config.identity.encryption_key.is_none() {
            error(
                "identity.encryption_key",
                "required for persisted identity inputs and writer administration".into(),
            );
        }
        if config.identity.directory.provider == DirectoryProvider::Scim
            && config.identity.directory.credentials.is_none()
        {
            error(
                "identity.directory.credentials",
                "SCIM requires a separate provider credential".into(),
            );
        }
    }
}

fn directory(config: &Config, error: &mut impl FnMut(&str, String)) {
    if config.identity.directory.provider == DirectoryProvider::Scim {
        if config
            .identity
            .directory
            .issuer
            .as_ref()
            .is_none_or(|v| v.is_empty())
        {
            error(
                "identity.directory.issuer",
                "SCIM requires its configured IdP issuer".into(),
            );
        }
        if config.identity.directory.verified_domains.is_empty() {
            error(
                "identity.directory.verified_domains",
                "SCIM requires corporate domains".into(),
            );
        }
    }
    if config.identity.directory.provider == DirectoryProvider::Keycloak {
        for (path, missing) in [
            (
                "identity.directory.endpoint",
                config.identity.directory.endpoint.is_none(),
            ),
            (
                "identity.directory.realm",
                config.identity.directory.realm.is_none(),
            ),
            (
                "identity.directory.credentials",
                config.identity.directory.credentials.is_none(),
            ),
            (
                "identity.directory.verified_domains",
                config.identity.directory.verified_domains.is_empty(),
            ),
            (
                "identity.encryption_key",
                config.identity.encryption_key.is_none(),
            ),
        ] {
            if missing {
                error(path, "required for Keycloak directory pulls".into());
            }
        }
    }
    if config.identity.directory.provider == DirectoryProvider::Snapshot
        && config.identity.directory.snapshot_file.is_none()
    {
        error(
            "identity.directory.snapshot_file",
            "required for snapshot directory pulls".into(),
        );
    }
}

fn writer_credentials(config: &Config, error: &mut impl FnMut(&str, String)) {
    let writer = &config.writer;
    if writer
        .credentials
        .iter()
        .any(|(env, id)| env.is_empty() || env.len() > 256 || id.is_empty() || id.len() > 256)
    {
        error(
            "writer.credentials",
            "credential references must be bounded and nonempty".into(),
        );
    }
}

fn sources(config: &Config, error: &mut impl FnMut(&str, String)) {
    let writer = &config.writer;
    let mut ids = BTreeSet::new();
    for (index, source) in config.sources.iter().enumerate() {
        let at = |field: &str| format!("sources[{index}].{field}");
        if !ids.insert(source.id.as_str()) {
            error(&at("id"), "duplicate source ID".into());
        }
        let intervals = &source.intervals;
        match source.permissions {
            PermissionsMode::Fixed => {
                if source.readers.is_empty() {
                    error(
                        &at("readers"),
                        "a fixed connection needs at least one reader token".into(),
                    );
                } else if let Err(message) = check_readers("readers", &source.readers) {
                    error(&at("readers"), message);
                }
                if intervals.permissions.is_some() || intervals.members.is_some() {
                    error(
                        &at("intervals"),
                        "a fixed connection makes no permission or member reads".into(),
                    );
                }
            }
            PermissionsMode::Sync => {
                if FIXED_ONLY_PROVIDERS.contains(&source.provider.as_str()) {
                    error(
                        &at("permissions"),
                        format!(
                            "{} connections always use permissions = \"fixed\"",
                            source.provider
                        ),
                    );
                }
                if !source.readers.is_empty() {
                    error(
                        &at("readers"),
                        "readers apply only to permissions = \"fixed\"".into(),
                    );
                }
                if intervals.permissions.is_none() {
                    error(
                        &at("intervals.permissions"),
                        "required when permissions = \"sync\"".into(),
                    );
                }
            }
        }
        for (field, interval) in [
            ("intervals.content", Some(intervals.content)),
            ("intervals.reconcile", Some(intervals.reconcile)),
            ("intervals.permissions", intervals.permissions),
            ("intervals.members", intervals.members),
        ] {
            let Some(interval) = interval else { continue };
            if interval.as_millis() == 0 {
                error(&at(field), "must be greater than zero".into());
            }
        }
        if let Some(limit) = source.document_max_bytes
            && writer.envelope_max_bytes <= limit
        {
            error(
                &at("document_max_bytes"),
                "writer.envelope_max_bytes must exceed it: a document never spans envelopes".into(),
            );
        }
        if let (Some(warning), Some(limit)) = (source.row_warning, source.row_error)
            && warning >= limit
        {
            error(&at("row_warning"), "must be below row_error".into());
        }
        for (table, value) in [("scope", &source.scope), ("settings", &source.settings)] {
            let mut inline = Vec::new();
            find_inline_secrets(
                &at(table),
                &Value::Object(value.clone()),
                false,
                &mut inline,
            );
            for (path, message) in inline {
                error(&path, message);
            }
            // A stored credential reaches the provider host's environment.
            let mut references = Vec::new();
            env_references(&at(table), &Value::Object(value.clone()), &mut references);
            for (path, name) in references {
                if writer.credentials.contains_key(&name)
                    && let Some(message) = host_env_refusal(&name)
                {
                    error(&path, message.into());
                }
            }
        }
    }
}
