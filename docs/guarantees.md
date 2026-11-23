# Guarantees

> **In short**
>
> - What fyrr promises about a write, a read and a storage outage, one
>   statement per row. Each row names the test that holds it as
>   `path::test_name`, or says `No test yet`.
> - A write is acknowledged once the head that readers serve holds it. The
>   head is replaced in one conditional write, so a reader sees a whole
>   publication or the one before it.
> - A `cached` read may lag a write by one version check. A `strong` read
>   includes every write acknowledged before it began, or fails.
> - `scripts/check-docs` checks that every named test exists. It does not
>   run it: the tests run in [`scripts/ci`](../CONTRIBUTING.md#verifying).

A statement here is a promise of the current build. Where a row says `No test
yet`, the behavior is in the code and in the page it links, and nothing holds
it against a change. A later change that adds a guarantee adds its row.

## Durable

A write is **durable** when the head that readers serve holds it.

| Statement | Held by |
| --- | --- |
| A `POST /v1/namespaces/{namespace}/documents` is answered `200` when the head that readers serve holds its rows, and the rows are then found by a read. A request refused for a document it holds does not create the namespace. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_upserts_searches_fetches_deletes_and_refuses_on_storage` |
| On local storage, what a prepare or a publication references is flushed before it is written. | `crates/fyrr/tests/dense_path/durability_syncs.rs::references_are_synced_before_real_prepare_and_publication` |
| A request answered `200` is never refused later. | No test yet |
| A `503 outcome_unknown` or `503 unavailable` says nothing about the outcome: the request may still be published, and repeating it is safe for the same IDs. | No test yet |

## Atomic

One request is published whole or refused whole, and the head is replaced in
one conditional write.

| Statement | Held by |
| --- | --- |
| The same `id` twice in a request is `400 duplicate_id`. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_upserts_searches_fetches_deletes_and_refuses_on_storage` |
| A `delete_where` or `patch_where` that matches more than 10,000 documents refuses the request (`409 rejected`) and changes nothing of it. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_refuses_a_filter_that_matches_too_many_documents_on_storage` |
| A change with a condition applies only to a stored document that meets it; the other changes of the request apply. Of many unpublished requests with a condition on one document, one wins. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_applies_a_change_only_when_its_condition_holds_on_storage`, `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_lets_one_of_many_unpublished_requests_win_its_condition_on_storage` |
| A committer killed at one of six storage boundaries (before a Lance version, a manifest, the serving head or the policy head is written, or after a Lance version or the serving head was written but before the answer came back) leaves the old head, or the whole new publication once the head was written. A rerun publishes exactly one publication. | `crates/fyrr/tests/dense_path/coordination.rs::coordination_crash_boundaries_on_storage` |
| A committer whose lease expired cannot replace the serving head of the committer that holds it. | `crates/fyrr/tests/dense_path/coordination.rs::coordination_stale_publish_preserves_authoritative_head_on_local_storage`, `crates/fyrr/tests/dense_path/coordination.rs::coordination_live_holder_is_busy_then_fenced_after_expiry_on_local_storage` |
| A namespace frozen after a request was accepted still commits the request whose envelope was durable before the switch; later writes are `409 namespace_read_only` and write nothing. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_freezes_a_namespace_and_commits_what_it_accepted_before_on_storage` |

## Isolation

There is no transaction across requests: requests are applied in the order
they are published, and the last one wins for an ID. A condition guards one
document against that order.

| Statement | Held by |
| --- | --- |
| A request pins one publication from start to finish; a reader that switches to a newer one keeps the pin of a request in flight. | `crates/fyrr/tests/dense_path/maintenance.rs::maintenance_reader_switch_keeps_inflight_pin_and_evicts_old_mirror_on_local_storage` |

## What a read sees

| Statement | Held by |
| --- | --- |
| A `cached` read (the default) is answered from the version the reader holds and makes no storage request of its own. A write acknowledged a moment ago may not show. | `crates/fyrr/tests/dense_path/storage_budget.rs::a_strong_read_checks_the_head_and_never_answers_from_a_held_view` |
| A `strong` read includes every write acknowledged before the request began, and reads the head once when it has not moved. | `crates/fyrr/tests/dense_path/storage_budget.rs::a_strong_read_checks_the_head_and_never_answers_from_a_held_view` |
| Until a running reader has switched to a new publication, it answers whole from the one it holds; it follows the head within its version-check interval. | `crates/fyrr/tests/dense_path/mirror_identity.rs::running_reader_with_a_shared_head_check_follows_the_head_within_its_interval` |
| A row the caller may not read is never returned. | `crates/fyrr/tests/dense_path/local_storage.rs::local_storage_publishes_and_serves_with_permission_filtering` |
| Every admitted result and error is checked against the serving head again before it is returned. | `crates/fyrr/tests/dense_path/tools.rs::tools_every_admitted_result_and_error_rechecks_identity_on_storage` |
| A published revocation takes effect with the next version check, at the latest two intervals after it was published. | No test yet |

## Object storage is unreachable

| Statement | Held by |
| --- | --- |
| A `strong` read whose head cannot be read answers `503 storage_unavailable` with `Retry-After: 1`. It is never answered from the held version; repeating it is safe. | `crates/fyrr/tests/dense_path/storage_budget.rs::a_strong_read_checks_the_head_and_never_answers_from_a_held_view` |
| A reader whose serving head is gone answers `storage_unavailable` and returns no document text. | `crates/fyrr/tests/dense_path.rs::dense_path_and_namespace_contracts` |
| A `fetch` whose locator index cannot be read answers `storage_unavailable` and returns no document. | `crates/fyrr/tests/dense_path/tools.rs::tools_fetch_refuses_unreadable_locator_index_on_storage` |
| A `cached` read keeps being answered from the held version while the held head is younger than two version-check intervals; an older one is read again, and a failed read fails the requests that waited for it. | No test yet |
| A write while storage is unreachable is `503 unavailable` or `503 outcome_unknown`, never `200`. | No test yet |
