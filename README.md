# Scryer

Scryer is a Python daemon that watches a GitHub repository for open issues with
a trigger label (default: `enhancement`), runs Codex in a dedicated git
worktree, and opens draft pull requests for human review.

## Quick Start

1. Create `config.toml` (see `config.example.toml`).
2. Ensure `gh` auth is configured for the target repo.
3. Ensure `codex` CLI is installed and accessible in `PATH`.
4. Run one cycle:

```bash
scryer run-once --config config.toml
```

5. Run continuously:

```bash
scryer daemon --config config.toml
```

## Commands

- `scryer status`: print SQLite status counts.
- `scryer run-once`: poll, claim one issue, run Codex, create/update PR state.
- `scryer daemon`: repeat the same loop with lease-aware recovery.
- `scryer doctor`: verify local environment readiness (`git`, `gh`, repo access, `codex`, paths).
