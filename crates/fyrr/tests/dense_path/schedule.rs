//! A schedule test of the permission pipeline across crash, takeover, rejection
//! and cleanup (#325), through the real worker, committer, reader and cleanup on
//! local storage, with `fyrr_core::clock`.
//!
//! Short schedules, drawn from a seed or written out, run reads that succeed,
//! fail midway, crash after their put or after the cursor write, a lease takeover
//! with a late put, a rejection, cleanup after the clock passes the age of an
//! envelope, a copy and a purge. A small oracle, independent of the writer, knows
//! what each read states; after every publication the rights a reader sees are
//! checked against it, and what the committer needs of the log must still be
//! there. A failure prints its seed and its trace.
use super::coordination::{command, worker_command, worker_sync};
use super::incremental::{
    fixture, fixture_host, input, publish, roots, row_readers, rows, source_roots, source_rows,
};
use super::*;
use fyrr_core::connector::{CursorState, DocumentPermission, SyncKind};
use fyrr_core::envelope::{EnvelopePayload, PreparedEnvelope, RejectionMarker};
use object_store::{PutMode, PutOptions};

/// The readers that the `i`th change of the source states: odd changes are of
/// `a`, even ones of `b`, and no two prefixes of the log state the same rights.
const COUNTS: [usize; 16] = [0, 0, 1, 1, 0, 2, 1, 3, 0, 4, 2, 0, 3, 1, 2, 2];

/// The rights after the first `length` changes of the source: the readers of
/// `a` and of `b`, which the content of the documents states, five each, until a
/// change does.
fn states_of(length: usize) -> (usize, usize) {
    let (mut a, mut b) = (5, 5);
    for (index, count) in COUNTS.iter().enumerate().take(length) {
        if index % 2 == 0 {
            a = *count;
        } else {
            b = *count;
        }
    }
    (a, b)
}

/// A deterministic generator: the same seed, the same schedule.
struct Dice(u64);
impl Dice {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) % n
    }
}

#[derive(Clone, Copy, Debug)]
enum Step {
    ReadOk,
    ReadFails,
    ReadCrashAfterCursor,
    CrashAfterPut,
    CrashAfterRejection,
    Takeover,
    LatePut,
    Oversize,
    AgeAndClean,
    Copy,
    OtherContent,
    Publish,
}

const STEPS: [(Step, u64); 12] = [
    (Step::ReadOk, 4),
    (Step::ReadFails, 1),
    (Step::ReadCrashAfterCursor, 1),
    (Step::CrashAfterPut, 1),
    (Step::CrashAfterRejection, 1),
    (Step::Takeover, 1),
    (Step::LatePut, 1),
    (Step::Oversize, 2),
    (Step::AgeAndClean, 1),
    (Step::Copy, 1),
    (Step::OtherContent, 1),
    (Step::Publish, 3),
];

struct Run {
    seed: u64,
    genesis: bool,
    trace: Vec<String>,
    worker: PathBuf,
    commit: PathBuf,
    root: FixtureRoot,
    epoch: u64,
    content: u32,
    other: u32,
    /// The changes of the source so far, as the readers each states.
    log: Vec<usize>,
    /// How much of the log the newest read that completed covers.
    completed: usize,
    /// The newest prefix of the log a publication has shown: rights never go back.
    seen: usize,
    /// The longest prefix of the log that any envelope states, a read that failed
    /// midway included: no publication shows more.
    upper: usize,
    /// The documents maintenance purged, until their content is fetched again.
    purged: [bool; 2],
    held: Option<CursorState>,
}

impl Drop for Run {
    #[allow(clippy::print_stderr)]
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "schedule seed {} (genesis: {}) failed; trace:\n  {}",
                self.seed,
                self.genesis,
                self.trace.join("\n  ")
            );
        }
    }
}

/// The stored cursor of the permissions of `incremental`, or a blank one before
/// any worker took it.
async fn cursor_or_blank(config: &Path) -> CursorState {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    match fyrr_reader::storage::read_contract(
        store.as_ref(),
        &ns.join("cursors").join("incremental-permissions.json"),
    )
    .await
    {
        Ok(state) => state,
        Err(_) => fyrr_writer::prepare::initial_cursor(
            &loaded.config,
            "incremental",
            SyncKind::Permissions,
        ),
    }
}

/// The cursor of the permissions of `incremental`, written as `state`.
async fn put_cursor(config: &Path, state: &CursorState) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    store
        .put(
            &ns.join("cursors").join("incremental-permissions.json"),
            write_contract(state).into(),
        )
        .await
        .unwrap();
}

/// The envelopes of the log, with their paths; a namespace needs no publication.
async fn log_of(config: &Path) -> Vec<(object_store::path::Path, PreparedEnvelope)> {
    use futures::TryStreamExt;
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let listed: Vec<_> = store
        .list(Some(&ns.join("envelopes")))
        .try_collect()
        .await
        .unwrap();
    let mut found = Vec::new();
    for object in listed {
        let envelope = fyrr_reader::storage::read_contract(store.as_ref(), &object.location)
            .await
            .unwrap();
        found.push((object.location, envelope));
    }
    found
}

/// The permission envelopes of the `incremental` source, oldest first.
async fn permission_envelopes(config: &Path) -> Vec<PreparedEnvelope> {
    let mut found: Vec<_> = log_of(config)
        .await
        .into_iter()
        .map(|(_, envelope)| envelope)
        .filter(|e| e.source_id == "incremental" && e.kind == SyncKind::Permissions)
        .collect();
    found.sort_by_key(|e| e.sequence);
    found
}

/// The envelopes of the sources other than `incremental` are removed from the
/// log of a namespace that has no publication, which leaves it one source.
async fn drop_other_sources(config: &Path) {
    let loaded = fyrr_core::config::load(config).unwrap();
    let (store, _) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    for (path, envelope) in log_of(config).await {
        if envelope.source_id != "incremental" {
            store.delete(&path).await.unwrap();
        }
    }
}

/// Readers that, as the row of a root states them, are over the bytes of a group.
fn many_readers() -> Vec<String> {
    (0..3000).map(|n| format!("grp:reader-{n:05}")).collect()
}

/// The configuration that makes `many_readers` a row over the bytes of a group.
fn small_groups(configs: &[&PathBuf]) {
    for config in configs {
        let text = std::fs::read_to_string(config).unwrap().replace(
            "[reader]",
            "[writer]\ndocument_max_bytes='64KiB'\nenvelope_max_bytes='128KiB'\n[writer.commit]\ngroup_max_bytes='128KiB'\n[reader]",
        );
        std::fs::write(config, text).unwrap();
    }
}

/// The change of the source at `index`, as an envelope a worker puts for it: the
/// next in the log of the cursor `state`, if its slot in the log is free.
async fn put_change(run: &Run, state: &CursorState, index: usize) -> bool {
    let loaded = fyrr_core::config::load(&run.worker).unwrap();
    let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let mut envelope: PreparedEnvelope = fyrr_core::parse_contract(include_bytes!(
        "../../../../fixtures/contracts/prepared-envelope/permissions.json"
    ))
    .unwrap();
    let (id, tag) = if index.is_multiple_of(2) {
        ("a", "ra")
    } else {
        ("b", "rb")
    };
    envelope.namespace.clone_from(&loaded.config.namespace.name);
    envelope.source_id = "incremental".into();
    envelope.sequence = state.sequence + 1;
    envelope.rejection_generation = state.rejection_generation;
    envelope.restore_sequence = state.restore_sequence;
    envelope.cursor.start.clone_from(&state.cursor);
    envelope.cursor.end = (index + 1).to_string();
    envelope.advances_cursor = true;
    envelope.base_head = None;
    envelope.created_at = fyrr_core::clock::now();
    let EnvelopePayload::Permissions(payload) = &mut envelope.payload else {
        unreachable!()
    };
    payload.complete_read = false;
    payload.container_grants = Vec::new();
    payload.documents = vec![DocumentPermission {
        root_id: id.into(),
        permission_revision: format!("p{index}-{}", run.log[index]),
        readers: (0..run.log[index])
            .map(|n| {
                fyrr_writer::apply::reader_token(
                    &loaded.config,
                    "incremental",
                    &format!("grp:{tag}{n}"),
                )
                .unwrap()
            })
            .collect(),
        containers: Vec::new(),
    }];
    envelope.envelope_id = fyrr_core::envelope::envelope_id(
        &envelope.namespace,
        &envelope.source_id,
        envelope.kind,
        envelope.sequence,
        envelope.rejection_generation,
        envelope.cursor.start.as_deref(),
    );
    // A late put cannot replace what the log holds: it lands only in a free slot.
    store
        .put_opts(
            &ns.join("envelopes")
                .join(format!("{}.json", envelope.envelope_id)),
            write_contract(&envelope).into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .is_ok()
}

impl Run {
    fn append(&mut self) {
        let index = self.log.len();
        self.log.push(COUNTS[index]);
    }

    /// Writes the source: its log of changes, and its documents. `oversize` is a
    /// change whose readers, this once, make a row over the bytes of a group.
    fn input(&mut self, oversize: Option<usize>, fail_at: Option<u64>) {
        self.epoch += 1;
        let revision = format!("r{}", self.content);
        let log: Vec<Value> = self
            .log
            .iter()
            .enumerate()
            .map(|(index, count)| {
                let (id, tag) = if index % 2 == 0 {
                    ("a", "ra")
                } else {
                    ("b", "rb")
                };
                let big = oversize == Some(index);
                json!({"id":id,
                    "revision":format!("p{index}-{}", if big { "big".to_owned() } else { count.to_string() }),
                    "readers": if big { many_readers() } else { (0..*count).map(|n| format!("grp:{tag}{n}")).collect() }})
            })
            .collect();
        let mut value = json!({"epoch":self.epoch,
            "content":[[
                {"id":"a","revision":&revision,"texts":["fixture document 0"],"readers":(0..5).map(|n| format!("grp:ra{n}")).collect::<Vec<_>>()},
                {"id":"b","revision":&revision,"texts":["fixture document 1"],"readers":(0..5).map(|n| format!("grp:rb{n}")).collect::<Vec<_>>()}]],
            "permission_log":log});
        if let Some(at) = fail_at {
            value["fail_permissions_at"] = json!(at);
        }
        input(&self.root, value);
    }

    fn read(&mut self, label: &str) {
        self.append();
        self.trace
            .push(format!("{label} change {}", self.log.len()));
        self.input(None, None);
        worker_sync(&self.worker, &["incremental", "--kind", "permissions"]);
        self.completed = self.log.len();
        self.upper = self.upper.max(self.completed);
    }

    async fn published(&self) -> bool {
        let loaded = fyrr_core::config::load(&self.commit).unwrap();
        fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .is_some_and(|head| head.content.is_some())
    }

    async fn step(&mut self, step: Step) {
        let published = self.published().await;
        match step {
            Step::ReadOk => self.read("read_ok"),
            Step::ReadCrashAfterCursor => {
                self.read("read_ok_then_crash_after_the_cursor");
                // The worker dies before the write that stamps the read.
                let mut state = cursor_or_blank(&self.worker).await;
                state.updated_at = chrono::DateTime::UNIX_EPOCH;
                put_cursor(&self.worker, &state).await;
            }
            Step::ReadFails => {
                self.trace.push("read_fails_midway".into());
                if self.log.len() + 2 <= COUNTS.len() {
                    self.append();
                    self.append();
                }
                self.input(None, Some(1));
                let output = worker_command(&self.worker)
                    .args(["writer", "sync", "incremental", "--kind", "permissions"])
                    .output()
                    .unwrap();
                assert!(!output.status.success(), "the read was to fail");
                // Its first page may have been put: that is a prefix of the log.
                self.upper = self.upper.max(self.log.len() - 1);
            }
            Step::Oversize => {
                self.trace.push("read_with_a_row_over_the_group".into());
                self.append();
                self.input(Some(self.log.len() - 1), None);
                worker_sync(&self.worker, &["incremental", "--kind", "permissions"]);
            }
            Step::CrashAfterPut => {
                self.trace.push("crash_after_put_before_cursor".into());
                let before = cursor_or_blank(&self.worker).await;
                self.read("read_ok_then_crash_after_the_put");
                // The log holds the read; the cursor is as it was before it.
                let mut state = before;
                state.lease = cursor_or_blank(&self.worker).await.lease;
                put_cursor(&self.worker, &state).await;
            }
            Step::CrashAfterRejection => {
                // A commit wrote the rejection marker of the newest read and died
                // before it published: the read is rejected, whatever a retry decides.
                let loaded = fyrr_core::config::load(&self.worker).unwrap();
                let head = fyrr_reader::storage::optional_head(&loaded.config)
                    .await
                    .unwrap()
                    .unwrap();
                let (applied, rejected) = fyrr_writer::commit::published_ids(&loaded.config, &head)
                    .await
                    .unwrap();
                let unresolved = permission_envelopes(&self.worker)
                    .await
                    .into_iter()
                    .rfind(|e| {
                        !applied.contains(&e.envelope_id) && !rejected.contains(&e.envelope_id)
                    });
                let Some(envelope) = unresolved else {
                    self.trace
                        .push("crash_after_rejection (nothing to reject)".into());
                    return;
                };
                self.trace.push("crash_after_rejection".into());
                let (store, ns) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
                // Only a read the committer refuses on its own can have a marker
                // written before a crash: a retry writes the same one. A marker for
                // an envelope that is valid ends it both applied and rejected on this
                // pipeline; that is the fault of #305's script 103.
                let oversize = matches!(&envelope.payload, EnvelopePayload::Permissions(p)
                    if p.documents.iter().any(|d| d.readers.len() > 100));
                if !oversize {
                    self.trace
                        .push("crash_after_rejection (nothing the committer refuses)".into());
                    return;
                }
                let marker = RejectionMarker {
                    schema_version: fyrr_core::envelope::ENVELOPE_CONTRACT_VERSION,
                    envelope_id: envelope.envelope_id.clone(),
                    source_id: envelope.source_id.clone(),
                    kind: envelope.kind,
                    sequence: envelope.sequence,
                    rejection_generation: envelope.rejection_generation,
                    restore_sequence: envelope.restore_sequence,
                    start_cursor: envelope.cursor.start.clone(),
                    reason: "envelope_bounds_exceeded".into(),
                };
                store
                    .put(
                        &ns.join("rejections")
                            .join(format!("{}.json", envelope.envelope_id)),
                        write_contract(&marker).into(),
                    )
                    .await
                    .unwrap();
            }
            Step::Takeover => {
                self.trace.push("lease_takeover".into());
                let loaded = fyrr_core::config::load(&self.worker).unwrap();
                let old = fyrr_writer::sync_lease::CursorLease::acquire(
                    &loaded.config,
                    "incremental",
                    SyncKind::Permissions,
                )
                .await
                .unwrap();
                self.held = Some(old.state().await.unwrap());
                old.release().await.unwrap();
            }
            Step::LatePut => {
                self.trace.push("late_put_of_the_old_worker".into());
                // The old worker had read the change its cursor names; it puts
                // that read now, and the put lands only if no worker has taken
                // its slot in the log since.
                if let Some(held) = self.held.take() {
                    let index: usize = held.cursor.as_deref().map_or(0, |c| c.parse().unwrap());
                    if index < self.log.len() && put_change(self, &held, index).await {
                        self.upper = self.upper.max(index + 1);
                    }
                }
            }
            Step::AgeAndClean if published => {
                self.trace
                    .push("clock_passes_max_envelope_age_then_cleanup".into());
                fyrr_core::clock::advance(std::time::Duration::from_secs(4 * 86_400)).unwrap();
                command(&self.commit, &["writer", "maintain"]);
                // Reads nothing published by now are expired and rejected, and
                // maintenance purges a document nobody may read, by design: a
                // later grant needs its content fetched again.
                let present = roots(&rows(&self.worker).await);
                for (index, root) in ["a", "b"].iter().enumerate() {
                    if !present.iter().any(|id| id == root) {
                        self.purged[index] = true;
                    }
                }
                self.check().await;
            }
            Step::Copy if published => {
                self.trace.push("copy".into());
                self.epoch += 1;
                let to = super::namespace_copy::elsewhere(
                    &self.worker,
                    &format!("schedule-{}-{}", self.seed, self.epoch),
                    None,
                );
                super::namespace_copy::copied(&self.worker, &to);
                let here = self.visible().await;
                let batch = rows(&to).await;
                let readers = |root: &str| {
                    if roots(&batch).iter().any(|id| id == root) {
                        row_readers(&batch, root).len()
                    } else {
                        0
                    }
                };
                assert_eq!(
                    (readers("a"), readers("b")),
                    here,
                    "the copy holds the rights that were published"
                );
            }
            Step::OtherContent if published && !self.genesis => {
                self.trace.push("other_source_content".into());
                self.other += 1;
                let text = format!("fixture document other {}", self.other);
                let vector: Vec<f32> = (0..1024)
                    .map(|d| (((self.other + 1000) * (d + 7)) as f32).sin())
                    .collect();
                self.root.embedding.insert_both(&text, vector);
                std::fs::write(self.root.join("sources/0.txt"), &text).unwrap();
                worker_sync(&self.worker, &["allowed", "--kind", "content"]);
                publish(&self.commit);
                let control = source_rows(&self.worker, "allowed").await;
                let row = roots(&control).iter().position(|id| id == "0.txt").unwrap();
                assert_eq!(
                    fyrr_writer::apply::strings(&control, "searchable_text")
                        .unwrap()
                        .value(row),
                    format!("fixture document other {}", self.other),
                    "another source publishes whatever the permissions of this one are"
                );
                self.check().await;
            }
            Step::Publish | Step::AgeAndClean | Step::Copy | Step::OtherContent => {
                self.trace.push("publish".into());
                publish(&self.commit);
                self.check().await;
            }
        }
    }

    async fn visible(&self) -> (usize, usize) {
        let batch = rows(&self.worker).await;
        let readers = |index: usize, root: &str| {
            if roots(&batch).iter().any(|id| id == root) {
                assert!(!self.purged[index], "the purged document {root} is back");
                row_readers(&batch, root).len()
            } else {
                assert!(
                    self.purged[index],
                    "the document {root} is not published: {:?}",
                    roots(&batch)
                );
                0
            }
        };
        (readers(0, "a"), readers(1, "b"))
    }

    /// Whether the rights seen are `wanted`, a purged document aside: it has no
    /// rights until its content is fetched again.
    fn same(&self, seen: (usize, usize), wanted: (usize, usize)) -> bool {
        (self.purged[0] || seen.0 == wanted.0) && (self.purged[1] || seen.1 == wanted.1)
    }

    /// The invariants after a publication.
    async fn check(&mut self) {
        let loaded = fyrr_core::config::load(&self.commit).unwrap();
        let head = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        // 1. The rights are the state after some prefix of the changes that were
        // read, never older than what was seen, and never one that no read stated.
        if head.content.is_some() {
            let seen = self.visible().await;
            let version = (self.seen..=self.upper).find(|v| self.same(seen, states_of(*v)));
            let version = version.unwrap_or_else(|| {
                panic!(
                    "the rights {seen:?} are not those after a prefix of {}..={} changes",
                    self.seen, self.upper
                )
            });
            self.seen = version;
        }
        // 2. No envelope is both applied and rejected.
        let (applied, rejected) = fyrr_writer::commit::published_ids(&loaded.config, &head)
            .await
            .unwrap();
        if let Some(id) = applied.intersection(&rejected).next() {
            panic!("{id} is applied and rejected");
        }
        // 3. `writer status` counts what the committer has pending.
        let status = command(&self.commit, &["writer", "status"]);
        let pending = fyrr_writer::commit::pending(&loaded.config, &head)
            .await
            .unwrap()
            .len();
        assert_eq!(
            status["pending_envelopes"].as_u64().unwrap() as usize,
            pending,
            "writer status and the committer disagree on what is pending"
        );
    }

    /// Once the faults stop, a valid read unblocks the source, and nothing the
    /// committer needs was lost to cleanup.
    async fn recover(&mut self) {
        // A rejection the commit made is seen by the worker only at its next
        // read: a valid read, repeated, always ends in the rights it states.
        let mut rounds = 0;
        loop {
            self.read("final_valid_read");
            for _ in 0..3 {
                publish(&self.commit);
            }
            rounds += 1;
            if self.published().await && self.same(self.visible().await, states_of(self.completed))
            {
                break;
            }
            assert!(rounds < 3, "a valid re-read did not unblock the source");
        }
        self.check().await;
        // A document nobody may read stays purged: its content brings nothing back.
        let wanted = states_of(self.completed);
        self.purged = [
            self.purged[0] && wanted.0 == 0,
            self.purged[1] && wanted.1 == 0,
        ];
        self.content += 1;
        let revision = format!("r{}", self.content);
        self.input(None, None);
        worker_sync(&self.worker, &["incremental", "--kind", "content"]);
        for _ in 0..3 {
            publish(&self.commit);
        }
        let batch = rows(&self.worker).await;
        let revisions = fyrr_writer::apply::strings(&batch, "source_revision").unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(
                revisions.value(row),
                revision,
                "the content of the source publishes"
            );
        }
        assert_eq!(
            self.visible().await,
            wanted,
            "a document purged comes back with the rights of the source"
        );
        self.check().await;
        let loaded = fyrr_core::config::load(&self.commit).unwrap();
        let head = fyrr_reader::storage::optional_head(&loaded.config)
            .await
            .unwrap()
            .unwrap();
        assert!(
            fyrr_writer::commit::pending(&loaded.config, &head)
                .await
                .unwrap()
                .is_empty(),
            "nothing is left pending"
        );
    }
}

/// The steps the dice of `seed` pick.
fn dealt(seed: u64, length: usize) -> Vec<Step> {
    let mut dice = Dice(seed);
    let total: u64 = STEPS.iter().map(|(_, weight)| weight).sum();
    (0..length)
        .map(|_| {
            let mut pick = dice.below(total);
            STEPS
                .iter()
                .find(|(_, weight)| {
                    let hit = pick < *weight;
                    pick = pick.saturating_sub(*weight);
                    hit
                })
                .map(|(step, _)| *step)
                .unwrap()
        })
        .collect()
}

async fn schedule(seed: u64, genesis: bool, steps: &[Step], purge: bool) {
    let (worker, commit, root) = if genesis {
        fixture_host(768, false)
    } else {
        fixture()
    };
    if genesis {
        drop_other_sources(&worker).await;
    }
    let mut run = Run {
        seed,
        genesis,
        trace: Vec::new(),
        worker,
        commit,
        root,
        epoch: 0,
        content: 0,
        other: 0,
        log: Vec::new(),
        completed: 0,
        seen: 0,
        upper: 0,
        purged: [false; 2],
        held: None,
    };
    small_groups(&[&run.worker.clone(), &run.commit.clone()]);
    // A namespace that is published has had a read: the first change of the source.
    if genesis {
        run.input(None, None);
        worker_sync(&run.worker, &["incremental", "--kind", "content"]);
    } else {
        run.append();
        run.input(None, None);
        worker_sync(
            &run.worker,
            &["incremental", "--kind", "content", "--kind", "permissions"],
        );
        run.completed = 1;
        run.upper = 1;
        run.seen = 1;
        publish(&run.commit);
    }
    for step in steps {
        run.step(*step).await;
    }
    if purge {
        run.trace.push("purge".into());
        publish(&run.commit);
        command(
            &run.commit,
            &["writer", "source", "purge", "incremental", "--confirm"],
        );
        assert!(source_roots(&run.worker, "incremental").await.is_empty());
        if !genesis {
            assert!(!source_roots(&run.worker, "allowed").await.is_empty());
        }
    } else {
        run.recover().await;
    }
}

/// Bounded schedules of reads that succeed, fail, crash or are refused, of lease
/// takeovers with late puts, of cleanup after the clock passes the age of an
/// envelope, of copy and of purge, over the real worker, committer and reader
/// (#325). A failure prints its seed and trace.
#[tokio::test]
#[ignore = "real CLI and local storage; scripts/ci"]
async fn schedule_reads_faults_cleanup_and_copy_keep_the_rights_exact_on_storage() {
    for (seed, genesis, steps, purge) in SCRIPTS {
        schedule(seed, genesis, steps, purge).await;
    }
    for (seed, purge) in SCHEDULES {
        schedule(seed, seed % 2 == 1, &dealt(seed, 6), purge).await;
    }
}

/// The sequences each fault of the log needs, as one short schedule each.
#[rustfmt::skip]
const SCRIPTS: [(u64, bool, &[Step], bool); 6] = [
    // A revoke waits when a read after it is refused for a row over the group.
    (101, false, &[Step::ReadOk, Step::Oversize, Step::Publish], false),
    // A commit wrote the marker of the read that was refused and died: the older
    // read, accepted, is applied without it.
    (109, false, &[Step::ReadOk, Step::Oversize, Step::CrashAfterRejection, Step::Publish], false),
    // The same, and a valid read after it.
    (103, false, &[Step::ReadOk, Step::Oversize, Step::CrashAfterRejection, Step::Publish, Step::ReadOk, Step::Publish], false),
    // A crash between the put and the cursor write.
    (104, false, &[Step::CrashAfterPut, Step::Publish, Step::ReadOk, Step::Publish], false),
    // Reads nobody published when the clock passes their age, and a late put.
    (105, false, &[Step::ReadOk, Step::ReadOk, Step::ReadFails, Step::AgeAndClean, Step::LatePut], false),
    (106, false, &[Step::ReadOk, Step::Takeover, Step::LatePut, Step::CrashAfterPut, Step::Publish, Step::ReadCrashAfterCursor, Step::Publish, Step::Copy], false),
];

const SCHEDULES: [(u64, bool); 4] = [(2, false), (3, false), (4, false), (5, true)];
