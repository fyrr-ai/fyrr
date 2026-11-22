//! A permission read is published whole or not at all. The worker that holds a
//! source's permission cursor lease ends a read in the same cursor write that
//! names its sequences (`CursorState::completed_from` to `completed_through`),
//! so a worker that lost its lease cannot end a read. The committer publishes
//! only those sequences.

use crate::Result;
use fyrr_core::config::Config;
use fyrr_core::connector::SyncKind;
use fyrr_core::envelope::PreparedEnvelope;
use std::collections::{BTreeMap, BTreeSet};

/// What the permission cursor of a source says about its reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reads {
    /// The first sequence of the read in progress.
    pub read_start: u64,
    pub completed_from: u64,
    pub completed_through: u64,
}

pub(crate) type Cursors = BTreeMap<String, Reads>;

/// The permission cursors of the sources that have permission envelopes among
/// `envelopes`: one read each, and none when nothing waits.
pub(crate) async fn cursors(config: &Config, envelopes: &[&PreparedEnvelope]) -> Result<Cursors> {
    let sources: BTreeSet<&str> = envelopes
        .iter()
        .filter(|e| e.kind == SyncKind::Permissions)
        .map(|e| e.source_id.as_str())
        .collect();
    let mut found = Cursors::new();
    for source in sources {
        if let Some(state) =
            crate::sync_lease::CursorLease::peek(config, source, SyncKind::Permissions).await?
        {
            found.insert(
                source.to_owned(),
                Reads {
                    read_start: state.read_start,
                    completed_from: state.completed_from,
                    completed_through: state.completed_through,
                },
            );
        }
    }
    Ok(found)
}

/// Whether `read` names the permission envelope `e`.
fn named(read: &Reads, e: &PreparedEnvelope) -> bool {
    (read.completed_from..=read.completed_through).contains(&e.sequence)
}

/// Keeps every envelope but the permission envelopes that the cursor of their
/// source does not name as a completed read. The envelopes of a read that is
/// running or failed are newer than `completed_through`; those of a read that
/// a `--full` read abandoned are older than `completed_from`. The named
/// envelopes are held for the next tick when one of their sequences is missing
/// (the listing is behind the cursor) or stands for several envelopes.
pub(crate) fn keep_completed_reads(envelopes: &mut Vec<PreparedEnvelope>, cursors: &Cursors) {
    let mut found: BTreeMap<&str, BTreeMap<u64, usize>> = BTreeMap::new();
    for e in envelopes.iter().filter(|e| e.kind == SyncKind::Permissions) {
        if cursors.get(&e.source_id).is_some_and(|read| named(read, e)) {
            *found
                .entry(&e.source_id)
                .or_default()
                .entry(e.sequence)
                .or_default() += 1;
        }
    }
    let complete: BTreeSet<String> = found
        .into_iter()
        .filter(|(source, sequences)| {
            // shortcut: a read that a commit group split has its first envelopes
            // published already, so the run starts at the first one still pending.
            // Upgrade: record the read's envelope count in the cursor.
            let (Some((lowest, _)), Some((highest, _))) =
                (sequences.first_key_value(), sequences.last_key_value())
            else {
                return false;
            };
            *highest == cursors[*source].completed_through
                && sequences.len() as u64 == highest - lowest + 1
                && sequences.values().all(|count| *count == 1)
        })
        .map(|(source, _)| source.to_owned())
        .collect();
    envelopes.retain(|e| {
        e.kind != SyncKind::Permissions
            || (complete.contains(&e.source_id)
                && cursors.get(&e.source_id).is_some_and(|read| named(read, e)))
    });
}

/// The unpublished permission envelopes that no read will ever publish: they
/// precede the read in progress and are not the completed one. Cleanup retires
/// them.
pub(crate) fn abandoned(envelopes: &[&PreparedEnvelope], cursors: &Cursors) -> BTreeSet<String> {
    envelopes
        .iter()
        .filter(|e| e.kind == SyncKind::Permissions)
        .filter(|e| {
            cursors
                .get(&e.source_id)
                .is_some_and(|read| e.sequence < read.read_start && !named(read, e))
        })
        .map(|e| e.envelope_id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(source: &str, sequence: u64) -> PreparedEnvelope {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/contracts/prepared-envelope/permissions.json");
        let mut envelope: PreparedEnvelope =
            fyrr_core::parse_contract(&std::fs::read(path).unwrap()).unwrap();
        envelope.source_id = source.into();
        envelope.sequence = sequence;
        envelope.envelope_id = format!("env-{source}-{sequence}");
        envelope
    }

    fn at(read_start: u64, completed: (u64, u64)) -> Cursors {
        Cursors::from([(
            "s".to_owned(),
            Reads {
                read_start,
                completed_from: completed.0,
                completed_through: completed.1,
            },
        )])
    }

    fn kept(sequences: &[u64], cursors: &Cursors) -> Vec<u64> {
        let mut envelopes: Vec<_> = sequences.iter().map(|n| envelope("s", *n)).collect();
        keep_completed_reads(&mut envelopes, cursors);
        envelopes.iter().map(|e| e.sequence).collect()
    }

    fn abandoned_of(sequences: &[u64], cursors: &Cursors) -> Vec<u64> {
        let envelopes: Vec<_> = sequences.iter().map(|n| envelope("s", *n)).collect();
        let refs: Vec<_> = envelopes.iter().collect();
        let gone = abandoned(&refs, cursors);
        sequences
            .iter()
            .copied()
            .filter(|n| gone.contains(&format!("env-s-{n}")))
            .collect()
    }

    #[test]
    fn a_read_is_released_only_when_the_cursor_names_it() {
        // 4 to 6 were written; the cursor has not recorded the end of the read.
        assert_eq!(kept(&[4, 5, 6], &at(4, (0, 0))), Vec::<u64>::new());
        assert_eq!(kept(&[4, 5, 6], &at(4, (4, 6))), [4, 5, 6]);
        // No cursor: nothing is released.
        assert_eq!(kept(&[4, 5, 6], &Cursors::new()), Vec::<u64>::new());
    }

    #[test]
    fn a_missing_or_duplicated_sequence_holds_the_read() {
        assert_eq!(kept(&[4, 6], &at(4, (4, 6))), Vec::<u64>::new());
        assert_eq!(kept(&[4, 5], &at(4, (4, 6))), Vec::<u64>::new());
        assert_eq!(kept(&[4, 5, 5, 6], &at(4, (4, 6))), Vec::<u64>::new());
    }

    #[test]
    fn a_read_running_beside_a_completed_one_is_not_released() {
        // The first read is complete at 4 to 6; the second began at 7 and its
        // first envelope is not in the listing yet.
        assert_eq!(kept(&[4, 5, 6, 8], &at(7, (4, 6))), [4, 5, 6]);
        assert!(abandoned_of(&[4, 5, 6, 8], &at(7, (4, 6))).is_empty());
    }

    #[test]
    fn a_read_that_a_full_read_abandoned_is_never_released() {
        // The read that wrote 4 and 5 failed; the --full read that began at 6
        // completed at 7. A marker or envelope the old worker writes later
        // changes nothing: only the cursor names a read.
        assert_eq!(kept(&[4, 5, 6, 7], &at(6, (6, 7))), [6, 7]);
        assert_eq!(abandoned_of(&[4, 5, 6, 7], &at(6, (6, 7))), [4, 5]);
        // A second envelope with a published sequence holds the read.
        assert_eq!(kept(&[4, 6, 6, 7], &at(6, (6, 7))), Vec::<u64>::new());
    }

    #[test]
    fn a_resumed_read_does_not_take_in_the_envelopes_before_a_full_read() {
        // Sequence 4 was written by a read that failed, 5 by a --full read that
        // failed; the read that continued the second one ended at 6.
        assert_eq!(kept(&[4, 5, 6], &at(5, (5, 6))), [5, 6]);
        assert_eq!(abandoned_of(&[4, 5, 6], &at(5, (5, 6))), [4]);
    }

    #[test]
    fn a_split_read_continues_from_its_first_pending_envelope() {
        // 4 and 5 are published; the rest of the read still waits.
        assert_eq!(kept(&[6, 7], &at(4, (4, 7))), [6, 7]);
    }

    #[test]
    fn other_kinds_and_sources_are_untouched() {
        let mut envelopes = vec![envelope("s", 9), envelope("other", 4)];
        envelopes[1].kind = SyncKind::Members;
        keep_completed_reads(&mut envelopes, &at(4, (4, 6)));
        assert_eq!(
            envelopes.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            [4]
        );
    }
}
