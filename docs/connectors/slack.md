# Slack connector

> **In short**
>
> - Reads public and private channels of one Slack workspace with a user OAuth
>   token and publishes conversation windows and threads.
> - Modes: `sync` (workspace and channel membership become readers) and
>   `fixed`.

## Setup

1. Create an internal Slack app for the workspace and install it for a
   dedicated service-account user. Use that user's OAuth token (`xoxp-…`);
   bot tokens are refused because thread replies need user scopes.
2. Request exactly these user scopes: `channels:read`, `groups:read`,
   `channels:history`, `groups:history`, `users:read`, `users:read.email`.
   No `search`, `files`, `im`, `mpim`, user-group, admin or write scopes.
3. The service user must be a member of every private channel to sync.
4. Put the token in an environment variable and note the workspace ID
   (`T…`, the `team_id` returned by `auth.test`); both are verified every run.

## Configuration

See [shared configuration](README.md#shared-configuration) for installation,
permission intervals, encryption keys and identity links.

```toml
[[sources]]
id = "slack"
type = "slack"
permissions = "sync"                     # or "fixed" with readers = [...]

[sources.settings]
token = { env = "FYRR_SLACK_TOKEN" }
workspace_id = "T0123456789"
window_gap_seconds = 300                 # messages closer than this form one window

[sources.scope]
include = ["platform-*", "C0123456789"]  # channel IDs or name globs, at most 64
exclude = ["platform-private-*"]         # exclude wins
channel_types = ["public_channel", "private_channel"]
bots = false                             # include bot messages
include_shared = false                   # Slack Connect / shared channels; fixed only
```

| Key | Meaning |
| --- | --- |
| `settings.token`, `workspace_id` | The user token and the workspace it must belong to. |
| `settings.window_gap_seconds` | 1–3600; the gap that splits channel messages into windows. |
| `scope.include`, `exclude` | Channel IDs or simple name globs; names are resolved to IDs every run. At most 64 selected channels per source; use several sources for more. |
| `scope.channel_types` | `public_channel` and/or `private_channel`. Direct messages are refused in both modes. |
| `scope.bots` | Include bot messages (also inside threads). |
| `scope.include_shared` | Opt into shared channels; allowed only under `fixed`. |

## Documents

- A **window** is one document of consecutive non-thread messages in a
  channel, split where the gap exceeds `window_gap_seconds` or the text
  exceeds 16 KiB, keyed by the first message's timestamp
  (`slack:<workspace>:<channel>:window:<ts>`). Window boundaries are kept in
  the cursor, so edits and deletions never merge or re-key neighbouring
  windows.
- A **thread** is one document of the parent and all replies
  (`slack:<workspace>:<channel>:thread:<parent ts>`), reread in full when
  it changes.
- Messages render as plain text with author, time and a permalink
  location; user and channel mentions are resolved to names where the
  token can see them. Join, leave, topic and other system messages are
  dropped. Files, attachments, canvases and unfurls are not fetched.
- History is read by creation time with an overlap. Old edits, deletions
  and new replies on old parents are found by a rotation over published
  windows and threads targeting 12 hours; large workspaces or strict rate
  tiers extend that.
- An archived channel stays readable. A channel the token can no longer
  open (`channel_not_found`, `not_in_channel`) leaves the scope: its grants
  are emptied and its documents removed at the next complete listing.

## Permissions

Under `sync`:

| Slack case | Readers on the channel container `slack:<workspace>:channel:<id>` |
| --- | --- |
| Public channel | the synthetic workspace group `grp:slack.com:<workspace>:workspace` (every active full member) plus guests who are proven channel members |
| Private channel | the active current members' `usr:slack.com:<workspace>:<user id>` tokens |
| Single- or multi-channel guest | only the channels they are members of |
| Deactivated user | no grant; reported inactive |
| Slack Connect or Enterprise Grid shared channel | refused under `sync`; opt in under `fixed` |
| Direct or group message | refused in both modes |
| Bot | no grant |
| Unknown identity, incomplete membership page | no grant; the permission pass fails ([failure handling](README.md#shared-configuration)) |

Membership comes from one complete paged snapshot per channel plus the
complete user list; the user list over 16,384 full members fails closed.
See [corporate identity links](README.md#shared-configuration) for member
e-mail handling. Display names are never identities.

Under `fixed` the configured readers apply; no membership snapshot is read.
`users.info` still resolves author names, so `users:read` is required in both
modes. Shared channels are the operator's responsibility.

## Limits

- Serial requests honoring 429 `Retry-After`; six attempts per request.
  Default page size 100 for customer-built internal apps; commercial
  non-Marketplace apps may be limited to 15 objects and one history request
  per minute, which makes large scopes slow.
- At most 100,000 messages or identities per run and 10,000 pages per
  feed; reaching a bound fails the pass.
- The cursor keeps at most 8 window anchors per channel and 64 in total;
  older boundaries are recomputed deterministically from the messages.
- No DM, file, search or Events API support.
