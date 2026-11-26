# Guarantees

> **In short**
>
> - What fyrr promises about a write, a read and a storage outage, one
>   statement per row. Each row names the test that holds it as
>   `path::test_name`, or says `No test yet`.
> - A write is acknowledged once the head that readers serve holds it. The
>   head is replaced in one conditional write, so a reader sees a whole
>   publication or the one before it.
> - A `cached` read may lag a write. A `strong` read includes every
>   publication acknowledged before it began, or fails.
> - `scripts/check-docs` checks that every named test exists. It does not
>   run it: the tests run in [`scripts/ci`](../CONTRIBUTING.md#verifying).

A statement here is a promise of the current build. Where a row says `No test
yet`, the behavior is in the code and in the page it links, and nothing holds
it against a change. A later change that adds a guarantee adds its row.

## Durable

A write is **durable** when the head that readers serve holds it.

| Statement | Held by |
| --- | --- |
| A `POST /v1/namespaces/{namespace}/documents` is answered `200` when the head that readers serve holds the request. That does not mean every change applied (a change whose condition is not met is counted in `skipped` and writes nothing), and it does not mean the next read shows it: a `cached` read can still serve the older publication, and only a `strong` read is promised to include it (see [What a read sees](#what-a-read-sees)). A request refused for a document it holds does not create the namespace. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_upserts_searches_fetches_deletes_and_refuses_on_storage` |
| On local storage, what a prepare or a publication references is flushed before it is written. | `crates/fyrr/tests/dense_path/durability_syncs.rs::references_are_synced_before_real_prepare_and_publication` |
| A request answered `200` is never refused later. | No test yet |
| A `503 outcome_unknown` or `503 unavailable` says nothing about the outcome: the request may still be published. Repeating it applies it again after every request published meanwhile, so a repeat can overwrite a later change to the same IDs. | No test yet |

## Atomic

One request is published whole or refused whole, and the head is replaced in
one conditional write.

| Statement | Held by |
| --- | --- |
| The same `id` twice in a request is `400 duplicate_id`. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_upserts_searches_fetches_deletes_and_refuses_on_storage` |
| A `delete_where` or `patch_where` that matches more than 10,000 documents refuses the request (`409 rejected`) and changes nothing of it. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_refuses_a_filter_that_matches_too_many_documents_on_storage` |
| The committer tests the condition of a change against the stored document when it applies the change, in the order of publication. A change whose condition holds applies; otherwise it is skipped and counted in `skipped`, the other changes of the request apply, and the answer is still `200`. A missing document meets the condition of an upsert, which creates it; a patch or a delete of it is skipped. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_applies_a_change_only_when_its_condition_holds_on_storage` |
| A condition does not exclude other requests: every request whose condition holds when it is applied succeeds. Where the first change makes the others' condition false (here `owner` equal to `null`), exactly one of the requests prepared against the same document applies and the others are `200` with `skipped`. | `crates/fyrr/tests/dense_path/write_api.rs::identity_write_api_lets_one_of_many_unpublished_requests_win_its_condition_on_storage` |
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
| A `cached` read (the default) is answered from the version the reader holds, and a publication made a moment ago may not show. When the reader's version check has not found a newer head, the read makes no storage request of its own. | `crates/fyrr/tests/dense_path/storage_budget.rs::a_strong_read_checks_the_head_and_never_answers_from_a_held_view` |
| A `cached` read can cost storage requests: a namespace opened by request path that has served nothing for `reader.version_check_interval` first reads the head and waits for that check ([HTTP JSON API](http-api.md#routes)). | No test yet |
| A `strong` read includes every publication acknowledged before the request began, and reads the head once when it has not moved. | `crates/fyrr/tests/dense_path/storage_budget.rs::a_strong_read_checks_the_head_and_never_answers_from_a_held_view` |
| Until a running reader has switched to a new publication, it answers whole from the one it holds or refuses. It then follows the head: to a new publication, and to a refusal once the namespace is deleted. No fixed time is promised; the test sets the interval to 1 s and waits up to 60 s. | `crates/fyrr/tests/dense_path/mirror_identity.rs::running_reader_with_a_shared_head_check_follows_the_head_within_its_interval` |
| A row the caller may not read is never returned. | `crates/fyrr/tests/dense_path/local_storage.rs::local_storage_publishes_and_serves_with_permission_filtering` |
| When the identity of the serving head changes while a `fetch` (also one with an invalid cursor), a `list_filters` or an ordered `search` runs, the request answers `authorization_unavailable` instead of the answer under the old grant. A request that ends past its deadline or cancelled answers `deadline_exceeded` or `cancelled` when the rights it started under did not change, also when its admission never finished; otherwise `authorization_unavailable`. | `crates/fyrr/tests/dense_path/tools.rs::tools_every_admitted_result_and_error_rechecks_identity_on_storage` |
| Not every error is checked again: some errors after admission, for example an unknown column, are answered without the final permission check. | No test yet |
| A published revocation takes effect when the reader reads the head that holds it ([Reader and writer](reader-writer.md#reader)). No test holds a bound on how late that is. | No test yet |

## Object storage is unreachable

| Statement | Held by |
| --- | --- |
| A `strong` read whose head cannot be read answers `503 storage_unavailable` with `Retry-After: 1`. It is never answered from the held version; repeating it is safe. | `crates/fyrr/tests/dense_path/storage_budget.rs::a_strong_read_checks_the_head_and_never_answers_from_a_held_view` |
| A stdio reader whose head check finds the serving head gone answers `storage_unavailable` and returns no document text. | `crates/fyrr/tests/dense_path.rs::dense_path_and_namespace_contracts_on_local_storage` |
| A `fetch` whose locator index cannot be read answers `storage_unavailable` and returns no document. | `crates/fyrr/tests/dense_path/tools.rs::tools_fetch_refuses_unreadable_locator_index_on_storage` |
| There is no fixed bound on how long a `cached` read keeps being answered from the held version while storage is unreachable: a removed head can stay cached, and a failed head check can drop it earlier. | No test yet |
| A write while storage is unreachable can be refused, end in `503 unavailable` or `503 outcome_unknown`, or still be answered `200` when its publication completed before the failure. | No test yet |
