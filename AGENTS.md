# Moonbite contributor and setup-agent rules

This is the standalone Moonbite repository. Do not read another local project,
Hermes profile, chat archive, memory store, or credential directory to guess
configuration.

Before changes, record Moonbite/Hermes commits, platform, Python, and
`git status --short`. Use an isolated `HERMES_HOME`. Never commit private
messages, memory/state ledgers, targets, coordinates, machine paths, endpoints,
tokens, cookies, OAuth data, or provider account details.

Setup agents must present these owner decisions instead of inventing values:

- timezone, daily anchor, and state location;
- enabled modules;
- model task keys and their host-owned routes;
- autonomy providers, permissions, and weights;
- host cron cadence, timeout, and cost tier;
- delivery adapter, consent, and target;
- retention, backup, and rollback policy.

Engineering invariants:

- defaults stay inert and module gates are enforced;
- controls run before Judge/model calls;
- Judge and state errors fail closed;
- visible effects require receipts; text generation is not delivery;
- autonomy never rerolls a selected failure;
- one-shot controls are consumed only after their promised terminal;
- model provider/fallback logic never enters Moonbite config;
- macOS and Linux share one code path unless a demonstrated host boundary
  requires an adapter.

Pull request conventions:

- Name branches for the change, not the author, coding agent, or an internal
  work item. Prefer purpose-based names such as `feat/<topic>`, `fix/<topic>`,
  or `docs/<topic>`.
- Use concise Conventional Commit-style PR titles (`type(scope): summary`),
  for example `feat(heartbeat): allow urgent kinds to bypass contact guards`.
- Keep public PR titles, descriptions, branch names, and commit messages
  self-contained; do not expose internal ticket identifiers.

Required verification:

```bash
.venv/bin/python -m compileall -q moonbite_plugin tests
.venv/bin/pytest
HERMES_REPO=/path/to/hermes-agent ./scripts/test-hermes-contract.sh
```

Do not change a public license, publish a release, alter repository visibility,
or install into a real companion instance without explicit owner approval.
