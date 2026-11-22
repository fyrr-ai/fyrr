//! A permission read is published whole or not at all: its envelopes are
//! committable only once the complete-read envelope that ends it exists, and
//! only the envelopes of the sequences that envelope names.

use fyrr_core::connector::SyncKind;
use fyrr_core::envelope::{EnvelopePayload, PreparedEnvelope};
use std::collections::{BTreeMap, BTreeSet};

/// The sequence of the first envelope of the read that `envelope` ends.
pub(crate) fn ended(envelope: &PreparedEnvelope) -> Option<u64> {
    match &envelope.payload {
        EnvelopePayload::Permissions(p) if p.complete_read => Some(p.first_sequence),
        _ => None,
    }
}

/// Keeps the permission envelopes of the reads that have ended, and every
/// other envelope. A read's envelopes are the ones of its source with the
/// sequences from the first one that its complete-read envelope names to that
/// envelope. They are held for the next listing when one of those sequences is
/// missing or stands for several envelopes. What no ended read names (the
/// envelopes of a read that failed and was abandoned by a `--full` read, or
/// that is still running) is not released.
pub(crate) fn keep_ended_reads(envelopes: &mut Vec<PreparedEnvelope>) {
    let mut sources: BTreeMap<&str, BTreeMap<u64, Vec<&PreparedEnvelope>>> = BTreeMap::new();
    for envelope in envelopes.iter().filter(|e| e.kind == SyncKind::Permissions) {
        sources
            .entry(&envelope.source_id)
            .or_default()
            .entry(envelope.sequence)
            .or_default()
            .push(envelope);
    }
    let mut released = BTreeSet::new();
    for sequences in sources.values() {
        for (&last, ended_by) in sequences {
            let [marker] = ended_by.as_slice() else {
                continue;
            };
            let Some(first) = ended(marker).filter(|first| *first <= last) else {
                continue;
            };
            let read: Vec<_> = sequences.range(first..=last).collect();
            // shortcut: a read that a commit group split has its first envelopes
            // published already, so the run starts at the first one still pending.
            // Upgrade: the complete-read envelope records the read's envelope count.
            let Some((lowest, _)) = read.first() else {
                continue;
            };
            let lowest = **lowest;
            if read.len() as u64 == last - lowest + 1 && read.iter().all(|(_, e)| e.len() == 1) {
                released.extend(read.iter().map(|(_, e)| e[0].envelope_id.clone()));
            }
        }
    }
    envelopes.retain(|e| e.kind != SyncKind::Permissions || released.contains(&e.envelope_id));
}

/// The envelopes of reads that were abandoned: not published, they are older
/// than the newest complete-read envelope of their source and no ended read
/// names them. Nothing will ever release them.
pub(crate) fn abandoned(
    envelopes: &[&PreparedEnvelope],
    published: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut sources: BTreeMap<&str, Vec<&PreparedEnvelope>> = BTreeMap::new();
    for envelope in envelopes.iter().filter(|e| e.kind == SyncKind::Permissions) {
        sources
            .entry(&envelope.source_id)
            .or_default()
            .push(envelope);
    }
    let mut abandoned = BTreeSet::new();
    for source in sources.values() {
        let reads: Vec<_> = source
            .iter()
            .filter_map(|e| ended(e).map(|first| (first, e.sequence)))
            .collect();
        let Some(newest) = reads.iter().map(|(_, last)| *last).max() else {
            continue;
        };
        abandoned.extend(
            source
                .iter()
                .filter(|e| {
                    e.sequence < newest
                        && !published.contains(&e.envelope_id)
                        && !reads
                            .iter()
                            .any(|(first, last)| (*first..=*last).contains(&e.sequence))
                })
                .map(|e| e.envelope_id.clone()),
        );
    }
    abandoned
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(source: &str, sequence: u64, ends: Option<u64>) -> PreparedEnvelope {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/contracts/prepared-envelope/permissions.json");
        let mut envelope: PreparedEnvelope =
            fyrr_core::parse_contract(&std::fs::read(path).unwrap()).unwrap();
        envelope.source_id = source.into();
        envelope.sequence = sequence;
        envelope.envelope_id = format!("env-{source}-{sequence}");
        let EnvelopePayload::Permissions(payload) = &mut envelope.payload else {
            unreachable!()
        };
        payload.complete_read = ends.is_some();
        payload.first_sequence = ends.unwrap_or(0);
        envelope
    }

    fn kept(envelopes: Vec<PreparedEnvelope>) -> Vec<u64> {
        let mut envelopes = envelopes;
        keep_ended_reads(&mut envelopes);
        envelopes.iter().map(|e| e.sequence).collect()
    }

    #[test]
    fn a_read_is_released_only_with_its_complete_read_envelope() {
        let read = |last: bool| {
            let mut read = vec![envelope("s", 4, None), envelope("s", 5, None)];
            if last {
                read.push(envelope("s", 6, Some(4)));
            }
            read
        };
        assert_eq!(kept(read(false)), Vec::<u64>::new());
        assert_eq!(kept(read(true)), [4, 5, 6]);
    }

    #[test]
    fn a_missing_sequence_holds_the_read() {
        let read = vec![
            envelope("s", 4, None),
            envelope("s", 6, None),
            envelope("s", 7, Some(4)),
        ];
        assert_eq!(kept(read), Vec::<u64>::new());
        let duplicated = vec![
            envelope("s", 4, None),
            envelope("s", 4, None),
            envelope("s", 5, Some(4)),
        ];
        assert_eq!(kept(duplicated), Vec::<u64>::new());
    }

    #[test]
    fn the_envelopes_of_an_abandoned_read_are_never_released() {
        // Sequences 4 and 5 belong to a read that failed; a --full read began at 6.
        let envelopes = vec![
            envelope("s", 4, None),
            envelope("s", 5, None),
            envelope("s", 6, None),
            envelope("s", 7, Some(6)),
            envelope("other", 4, None),
        ];
        assert_eq!(kept(envelopes.clone()), [6, 7]);
        let refs: Vec<_> = envelopes.iter().collect();
        let gone = abandoned(&refs, &BTreeSet::from(["env-s-7".to_owned()]));
        assert_eq!(
            gone,
            BTreeSet::from(["env-s-4".to_owned(), "env-s-5".to_owned()])
        );
    }

    #[test]
    fn a_resumed_read_releases_what_the_failed_one_wrote() {
        // The read failed after 4 and 5; the one that resumed it ended at 7.
        let envelopes = vec![
            envelope("s", 4, None),
            envelope("s", 5, None),
            envelope("s", 6, None),
            envelope("s", 7, Some(4)),
        ];
        assert_eq!(kept(envelopes.clone()), [4, 5, 6, 7]);
        let refs: Vec<_> = envelopes.iter().collect();
        assert!(abandoned(&refs, &BTreeSet::new()).is_empty());
    }

    #[test]
    fn a_split_read_continues_from_its_first_pending_envelope() {
        // 4 and 5 are published; the read's remaining envelopes are still pending.
        assert_eq!(
            kept(vec![envelope("s", 6, None), envelope("s", 7, Some(4))]),
            [6, 7]
        );
    }
}
