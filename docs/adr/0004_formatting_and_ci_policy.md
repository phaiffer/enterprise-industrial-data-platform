# ADR 0004: Formatting and CI Policy

## Status
Accepted

## Context
The repository needs to be reviewer-friendly for portfolio use while keeping CI fast and deterministic. Heavy linting/tooling or Docker-based CI would increase execution time and reduce contributor velocity.

## Decision
Adopt lightweight formatting and lint standards with local/pre-commit enforcement:
- `ruff` for linting and import sorting
- `ruff format` and `black` for Python formatting
- `pre-commit` hooks for whitespace/YAML hygiene

CI scope remains Mode 1 only:
- `make setup`
- `make run-all`
- `make dbt-test`
- `make dq`

No Docker Compose steps are added to CI.

## Consequences
Positive:
- Cleaner diffs and consistent style across scripts and modules
- Faster review cycles and higher readability for recruiters
- Stable CI runtime focused on core Lakehouse correctness

Tradeoffs:
- Mode 2 infra health is validated locally, not in CI
- Developers should run `make fmt` and `make lint` before opening PRs
