# Publish documents

Publishing makes the documents of a source searchable: `fyrr writer sync`
reads the source and prepares what changed, and `fyrr writer commit` publishes
it. Readers see a new, changed or deleted document only after the commit.

## Sub-features

- `publish-add` makes a new file searchable and fetchable.
- `publish-edit` replaces the text of a changed file.
- `publish-delete` removes a deleted file from search and fetch.
- `publish-dry-run` runs the source's provider, validates its output and writes nothing.
- `publish-two-steps` keeps a synced, uncommitted change invisible to readers.
- `publish-status` shows pending work in `fyrr writer status`.

## How to get to it (user POV)

- Edit files in the folder a `local-files` source points at.
- Run `fyrr writer sync`, then `fyrr writer commit`, in a terminal.
- Run `fyrr writer sync --dry-run` to preview.
- Run `fyrr writer status` to see what is pending.

## Driving it with control-fyrr

Preconditions:

- Profile `notes` at the baseline, and `env.sh` loaded.
- The instance's document folder is `$FYRR_VERIFY_DOCS`, a variable that
  exists only inside `control-fyrr run`.

- **Baseline.** List the source folder. Run `control-fyrr run publish-baseline -- sh -c 'ls "$FYRR_VERIFY_DOCS/handbook"'`. It prints `backup.md` and `deploys.md` and nothing else.
- **Read before.** Search for the document that does not exist yet. Run `control-fyrr run publish-before -- fyrr reader search 'Who is on call this week?' --limit 5 | docs`. The output lists `notes/backup.md` and `notes/deploys.md` and no `notes/oncall.md`.
- **Add a file.** Write a new document into the source folder. Run `control-fyrr run publish-add-file -- sh -c 'printf "The on-call engineer rotates every Monday at noon.\n" > "$FYRR_VERIFY_DOCS/handbook/oncall.md"'`. Exit code `0`.
- **Dry run.** Preview the sync. Run `control-fyrr run publish-dry-run -- fyrr writer sync --dry-run`. Stdout starts with `dry run: 1 source(s), each in its own worker; nothing is written`, has a `content` line with `upserts=3` and ends with `dry run: 1 source(s) passed, 0 failed`.
- **Dry run wrote nothing.** Read the status. Run `control-fyrr run publish-status-dry -- fyrr writer status | jq -c '{envelopes, pending_envelopes}'`. It prints `{"envelopes":2,"pending_envelopes":0}`.
- **Sync.** Prepare the change. Run `control-fyrr run publish-sync -- fyrr writer sync | jq -c '{failed, upserts: .stats.upserts}'`. It prints `{"failed":false,"upserts":1}`.
- **Not visible before the commit.** Repeat the search and read the status. Run `control-fyrr run publish-uncommitted -- fyrr reader search 'Who is on call this week?' --limit 5 | docs` and `control-fyrr run publish-status-sync -- fyrr writer status | jq -c '{envelopes, pending_envelopes}'`. The search still has no `notes/oncall.md`; the status prints `{"envelopes":3,"pending_envelopes":1}`.
- **Commit.** Publish. Run `control-fyrr run publish-commit -- fyrr writer commit`. Exit code `0` and stdout is one JSON object with `publication_id`, `lance_version` and one entry in `envelope_ids`.
- **Visible after the commit.** Search over the CLI and fetch over MCP. Run `control-fyrr run publish-after -- fyrr reader search 'Who is on call this week?' --limit 5 | docs` and `control-fyrr mcp publish-after-fetch fetch '{"source_id":"notes","root_id":"oncall.md"}' | texts`. The search lists `notes/oncall.md` first; the fetch prints `The on-call engineer rotates every Monday at noon.`
- **Edit and delete.** Change the new file and remove another. Run `control-fyrr run publish-edit-files -- sh -c 'printf "The on-call engineer rotates every Friday at noon.\n" > "$FYRR_VERIFY_DOCS/handbook/oncall.md"; rm "$FYRR_VERIFY_DOCS/handbook/deploys.md"'`. Exit code `0`.
- **Publish the edit and the delete.** Sync and commit. Run `control-fyrr run publish-sync-2 -- fyrr writer sync | jq -c '{upserts: .stats.upserts, deletes: .stats.deletes}'` and `control-fyrr run publish-commit-2 -- fyrr writer commit`. The sync prints `{"upserts":1,"deletes":1}`; the commit exits `0`.
- **Nothing pending.** Read the status after the last commit. Run `control-fyrr run publish-status-final -- fyrr writer status | jq -c '{envelopes, pending_envelopes}'`. It prints `{"envelopes":4,"pending_envelopes":0}`.
- **Proof.** Read the changed documents back. Run `control-fyrr mcp publish-proof-edited fetch '{"source_id":"notes","root_id":"oncall.md"}' | texts`, `control-fyrr mcp publish-proof-deleted fetch '{"source_id":"notes","root_id":"deploys.md"}' | jq -c '.documents'` and `control-fyrr http publish-proof-http POST /v1/namespaces/notes/query '{"query":"on call","limit":5,"consistency":"strong"}' | docs`. The first prints the `Friday` text, the second `[]`, and the HTTP search lists `notes/oncall.md` and `notes/backup.md` and no `notes/deploys.md`.

## Gotchas

- The CLI and each `control-fyrr mcp` session start a new reader and see a
  commit at once. The long-running HTTP reader checks for a new publication
  every 30 seconds: without `"consistency":"strong"` in the JSON body, an
  HTTP read just after a commit still answers from the older publication.
- Every `fyrr writer` command prints `warning: sources[0].permissions: fixed
  connection: …` on stderr. It describes the example configuration and is not
  a failure.
- `fyrr writer sync --dry-run` prints text, not JSON, and counts every
  document of the source (`upserts=3`); the real sync that follows counts
  only what changed (`upserts=1`). `fyrr writer sync` prints JSON whose
  top-level `stats` are those of the content kind.
- `fyrr writer status` counts are cumulative: `envelopes` grows by one per
  synced change set; only `pending_envelopes` returns to `0` after a commit.
- A sync with nothing changed and a commit with nothing pending both exit `0`.
- Edit only the copies under `$FYRR_VERIFY_DOCS`. The files in
  `examples/quickstart/` are tracked, and the instance does not read them.
- This recipe changes the instance. `cleanup` and `launch` again before a
  recipe that needs the baseline.
- The toy embeddings match words. Query with words of the document; a
  paraphrase can rank another document first.
