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

To keep a persistent live log, add `--log-file` and tail it:

```bash
scryer daemon --config config.toml --log-file ./.scryer/daemon.log
tail -f ./.scryer/daemon.log
```

If you need to target a different local checkout, pass `--repo-root`:

```bash
scryer run-once --config config.toml --repo-root /path/to/local/checkout
```

## Commands

- `scryer status`: print SQLite status counts.
- `scryer run-once`: poll, claim one issue, run Codex, create/update PR state.
- `scryer daemon`: repeat the same loop with lease-aware recovery.
- `scryer doctor`: verify local environment readiness (`git`, `gh`, repo access, `codex`, paths).
- `scryer clean`: reset local runtime state (managed worktrees, run logs, and SQLite DB).

All commands accept `--repo-root` to control which local git repository is used
for git/worktree operations and local doctor checks.
All commands also accept `--log-level` and `--log-file` for runtime visibility.
