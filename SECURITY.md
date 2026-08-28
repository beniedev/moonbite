# Security policy

Moonbite is a pre-alpha and has no supported stable release yet. Do not
open a public issue containing credentials, private messages, personal memory,
session identifiers, platform targets, coordinates, state files, or logs.
Report a suspected vulnerability privately to the repository owner through
GitHub's private security-reporting channel when it is enabled.

## Trust and data boundary

- Model-facing tools are untrusted callers. Trusted source/provenance and
  cadence-bypass authority come from operator or deployment surfaces.
- Model-generated text is not evidence that a message was delivered.
- Hermes owns credentials, model routes, gateway execution, cron, and platform
  transports. Moonbite does not copy those values into its configuration.
- Local state is owner-only on supported POSIX hosts but may contain private
  text. Operators own retention, backup, and deletion.

## Release checks

Before any candidate tag or visibility change, the repository owner must
separately verify the current tree, reachable history, Git identity, remote
metadata, and built artifacts contain no private or secret material. Operator
inputs and maintainer-only release procedures are intentionally not shipped in
this repository.

A deployment candidate must satisfy
[DEPLOYMENT_COMPATIBILITY.md](DEPLOYMENT_COMPATIBILITY.md); repository CI does
not replace a natural-cycle check.

The repository uses the MIT License. Publishing a tag or changing repository
visibility remains a separate owner decision.

See [SETUP.md](SETUP.md) for testing and setup workflows.
