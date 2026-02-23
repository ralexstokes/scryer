# AGENTS.md

Repository conventions for human and automated coding agents.

## Project Intent

Scryer is a Python daemon that polls a GitHub repository for labeled issues, runs
Codex in an isolated git worktree, and opens or updates draft pull requests.

## Stack and Layout

- Python `>=3.14`
- Packaging: `setuptools` with `src/` layout
- Runtime dependencies: standard library only (add dependencies only when justified)
- CLI entrypoint: `scryer` -> `scryer.cli:main`

Key modules:

- `src/scryer/cli.py`: command wiring (`status`, `run-once`, `daemon`, `doctor`, `clean`)
- `src/scryer/config.py`: TOML/env config loading and defaults
- `src/scryer/daemon.py`: orchestration loop, retries, and lease-aware processing
- `src/scryer/runner.py`: worktree lifecycle, Codex invocation, commit/push flow
- `src/scryer/db.py`: SQLite state and leasing
- `src/scryer/gh.py`: GitHub CLI integration

## Local Environment

- Optional dev shell: `nix develop -c zsh`
- Required external tools for full behavior: `git`, `gh`, `codex`

## Working Rules

1. Keep changes minimal and scoped to the requested task.
2. Do not modify unrelated files.
3. Preserve existing CLI behavior unless a change is explicitly required.
4. If config surface changes, update all relevant places:
   - `config.example.toml`
   - `README.md` (for user-visible behavior)
   - `src/scryer/config.py` parsing/defaults
5. Keep typing explicit (`from __future__ import annotations`, typed signatures).
6. Follow existing logging style with concrete context fields (for example `issue=%s status=%s`).
7. Prefer small, reviewable commits and straightforward control flow over clever abstractions.

## Safety-Critical Areas

- `src/scryer/runner.py` and `src/scryer/cli.py` (`clean`) perform destructive worktree cleanup.
- Do not broaden deletion/removal scope without explicit requirements.
- Do not run remote-mutating commands (for example branch pushes) during local verification unless requested.

## Verification Expectations

Run the cheapest relevant checks for your change. At minimum:

```bash
python -m compileall src
python -m scryer --help
```

If CLI options/subcommands changed, also run:

```bash
python -m scryer <subcommand> --help
```

If config loading changed, verify with an explicit config path (not defaults only).

## Testing Status

- There is currently no dedicated test suite in this repository.
- If a testing framework is added, prefer small focused tests around changed behavior.
