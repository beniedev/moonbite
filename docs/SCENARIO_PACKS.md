# Scenario packs

Moonbite resolves an optional data-only scenario pack before validating the
existing behavior configuration. Hermes places the public selector beside the
user configuration:

```yaml
plugins:
  entries:
    moonbite:
      settings:
        scenario_pack: null
        config:
          modules:
            heartbeat: false
```

An omitted or `null` selector keeps the legacy `normalize_config` path exactly
unchanged. A non-null selector must be a fixed catalog key matching
`[a-z][a-z0-9-]{0,63}`; it is never treated as a filesystem path or URL.

Pack resources are JSON files inside the installed
`moonbite_plugin.scenario_packs` package. The catalog maps public names to
fixed resource names and is intentionally empty in Phase 1. Therefore no
production pack is selectable yet; `companion` and other packs belong to Phase
2. Existing `config/presets/*.yaml` files remain installation examples, not
runtime pack resources.

This JSON is package data maintained by Moonbite, not a beginner-facing
configuration surface. A future setup flow should let people choose a scenario
and answer only the host-required questions; users are not expected to edit
pack JSON.

Each pack uses the `moon.scenario-pack.v1` envelope and an `overlay` mapping.
Only module gates for heartbeat, autonomy, panel, and memory plus their
corresponding behavior sections are allowed. State paths, timezone, delivery,
model routes, `config_version`, and unknown roots are rejected before
registration.

When a pack is selected, the resolver deep-merges `pack < user`: mappings
recurse, lists/scalars/`null` replace, and an explicit empty mapping replaces
the pack mapping. The result then goes through the existing strict normalizer.
Pack, user, and normalizer-default leaves receive deterministic JSON Pointer
provenance (`pack:<name>`, `user`, or `default`); pointers escape `~` and `/`.

Runtime keeps the complete effective configuration internally. Doctor output
uses a redacted resolution view: timezone, state directory, delivery target,
and model-route aliases are replaced with `<redacted>` when non-null. The
model-facing status surface exposes only the selected public pack name.
