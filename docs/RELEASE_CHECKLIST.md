# Release Checklist (Owner Actions)

This checklist is an authoritative guide for repository owners preparing a public or tagged release of Moonbite. All steps must be executed manually and verified by the repository owner.

> [!IMPORTANT]
> **Owner-Only Gate:**
> Do not publish a release, change repository visibility, alter license terms, or install into a production companion instance without completing every item below.

---

## 1. Pre-Release Verification & Static Checks

- [ ] **Full Test Suite & Compilation:**
  ```bash
  .venv/bin/python -m compileall -q moonbite_plugin tests
  .venv/bin/pytest
  .venv/bin/ruff check moonbite_plugin tests scripts/check-hermes-contract.py --select F,E9
  .venv/bin/ruff format --check moonbite_plugin tests scripts/check-hermes-contract.py
  ```

- [ ] **Pinned Hermes Contract Validation:**
  Verify against official pinned Hermes commit `987064caa4f8845f605ac7346fed5b72fddfb21c`:
  ```bash
  HERMES_REPO=/path/to/hermes-agent \
  HERMES_EXPECTED_COMMIT=987064caa4f8845f605ac7346fed5b72fddfb21c \
  MOONBITE_TEST_HOME=.hermes-test \
  ./scripts/test-hermes-contract.sh
  ```

- [ ] **Current Upstream Drift Detection:**
  Resolve and record the then-current upstream Hermes commit, then run the
  public-behavior contract test against it. Record the tested SHA from the
  latest scheduled/manual workflow log.

---

## 2. Repository Privacy Gate

- [ ] **Maintainer Review Complete:**
  The repository owner has separately verified the current tree, reachable
  history, Git identity, remote metadata, and built artifacts contain no
  private or secret material. Maintainer-only tooling and inputs are not part
  of this repository.

---

## 3. Package Build & Wheel Cleanliness Smoke Test

- [ ] **Build Wheel:**
  ```bash
  python -m pip wheel --disable-pip-version-check --no-deps --wheel-dir dist/release .
  ```

- [ ] **Verify Wheel Contents:**
  Inspect built `.whl` archive to ensure it contains **only** the `moonbite_plugin/` Python package and `dist-info` metadata:
  ```bash
  python -m zipfile -l dist/release/*.whl
  ```
  Ensure that root `config/`, `docs/`, `examples/`, and `tests/` are excluded from the runtime wheel package.

- [ ] **Verify Entry Point Definition:**
  Confirm `pyproject.toml` contains:
  ```toml
  [project.entry-points."hermes_agent.plugins"]
  moonbite = "moonbite_plugin"
  ```

- [ ] **Clean Wheel Installation Smoke Test:**
  Verify package contents, import origin, entry-point discovery, `register`, and opt-in loading in the repository's isolated runner:
  ```bash
  HERMES_REPO=/path/to/isolated/hermes-agent \
  HERMES_EXPECTED_COMMIT=987064caa4f8845f605ac7346fed5b72fddfb21c \
  ./scripts/test-clean-wheel.sh
  ```

- [ ] **Clean Source-Install Lifecycle:**
  In an empty isolated `HERMES_HOME`, verify install-disabled, Doctor, config,
  enable, fresh-process surfaces, safe smoke, disable, and state retention:
  ```bash
  HERMES_REPO=/path/to/isolated/hermes-agent \
  HERMES_EXPECTED_COMMIT=987064caa4f8845f605ac7346fed5b72fddfb21c \
  MOONBITE_TEST_HOME=/path/to/empty/test-home \
  ./scripts/test-clean-install.sh
  ```

---

## 4. Manifest Compatibility & Schema Decisions

- [ ] **Manifest Version Check:**
  Confirm `plugin.yaml` retains `manifest_version: 1` and `kind: standalone` for compatibility with the pinned Hermes plugin installer.
- [ ] **Future Manifest v2 Transition:**
  Manifest v2 adoption is reserved as a future release compatibility milestone once upstream Hermes releases and installer tooling officially mandate v2.

---

## 5. Tagging, Artifact Hashing & GitHub Release

- [ ] **Tagging Release Commit:**
  After explicit owner authorization, create an immutable signed tag for the
  approved release commit. Replace both placeholders; do not execute these as
  written:
  ```bash
  git tag -s <version-tag> <approved-release-commit> -m "Release <version-tag>"
  git push origin <version-tag>
  ```

- [ ] **Compute Artifact SHA-256 Hashes:**
  ```bash
  sha256sum dist/* > dist/SHA256SUMS
  ```

- [ ] **GitHub Repository Settings (Manual Owner Configuration):**
  - Verify branch protection rules on `main`.
  - Ensure all CI status checks are marked as required.
  - Review public repository visibility and license disclosures.
  - Record the designated rollback commit SHA.
