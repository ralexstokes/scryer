# Scryer

Scryer is a Python daemon that watches a GitHub repository for open issues with
a trigger label (default: `enhancement`), runs Codex in a dedicated git
worktree, and opens draft pull requests for human review.

## Quick Start

1. Create `~/.config/scryer/config.toml` (see `config.example.toml`), or pass an explicit `--config` path.
2. Ensure `gh` auth is configured for the target repo.
3. Ensure `codex` CLI is installed and accessible in `PATH`.
4. Run one cycle:

```bash
scryer run-once
```

5. Run continuously:

```bash
scryer daemon
```

To keep a persistent live log, add `--log-file` and tail it:

```bash
scryer daemon --log-file ./.scryer/daemon.log
tail -f ./.scryer/daemon.log
```

If you need to target a different local checkout, pass `--repo-root`:

```bash
scryer run-once --repo-root /path/to/local/checkout
```

If you need to run a specific issue number directly, pass `--issue`:

```bash
scryer run-once --issue 123
```

## Commands

- `scryer status`: print SQLite status counts.
- `scryer run-once`: poll, claim one issue, run Codex, create/update PR state.
  Use `--issue <number>` to target one specific issue.
- `scryer daemon`: repeat the same loop with lease-aware recovery.
- `scryer doctor`: verify local environment readiness (`git`, `gh`, repo access, `codex`, paths).
- `scryer clean`: reset local runtime state (managed worktrees, run logs, and SQLite DB).

All commands accept `--repo-root` to control which local git repository is used
for git/worktree operations and local doctor checks.
All commands also accept `--log-level` and `--log-file` for runtime visibility.
