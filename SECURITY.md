# Security policy

fyrr decides which documents an AI agent may see, so a flaw that lets a
caller read something they should not is the most serious class of bug in
this project.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://github.com/fyrr-ai/fyrr/security/advisories/new)
("Report a vulnerability" on the repository's **Security** tab). Do not open
a public issue, pull request or discussion for a suspected vulnerability.

A useful report says what an attacker can do, which build and configuration
it needs (full or reader-only, stdio or HTTP, provider and permission mode),
and how to reproduce it, ideally against the local S3 fixture
(`scripts/s3-fixture`) with synthetic data. Never include real documents,
credentials or personal data.

We acknowledge reports, keep you informed while we investigate, and credit
you in the advisory unless you prefer otherwise. We ask that you give us a
reasonable time to release a fix before you disclose the issue.

## Scope

In scope, for example:

- a caller receiving text, metadata, counts or existence signals from a
  document their tokens do not cover, or from a hidden or retired source;
- authorization continuing after it should fail closed;
- secrets, document text, identities or queries appearing in logs, errors,
  telemetry or other output;
- HTTP MCP authentication or token-validation bypasses;
- the reader-only build loading Python or writer code.

Out of scope: behavior that [docs/security.md](docs/security.md) states as
the deployment's responsibility, such as protecting the S3 bucket, the
reader mirror, provider code and the embedding or rerank services.

## Supported versions

fyrr is pre-1.0. Fixes land on the `main` branch and ship in the next
release; there are no maintained release branches.
