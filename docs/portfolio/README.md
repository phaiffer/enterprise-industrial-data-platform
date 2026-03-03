# Portfolio Pack

The Portfolio Pack generates deterministic, website-ready sales assets from the Mode 1 pipeline outputs.

## Run Commands

Primary full run (setup + pipeline + tests + DQ + export):

```bash
make portfolio
```

Export only (reuse existing pipeline outputs):

```bash
make portfolio-export
```

Export with an explicit run id:

```bash
make portfolio-export RUN_ID=20260303T010000Z
```

Lightweight validation mode (for CI or local quick checks, no latest pointer update):

```bash
make portfolio-validate
```

Print latest chart/manifest paths (no OS-specific open):

```bash
make portfolio-open
```

Clean generated runs while preserving committed sample artifacts:

```bash
make portfolio-clean
```

## Output Layout

Each run writes to:

```text
docs/portfolio/exports/<run_id>/
  charts/
  reports/
  manifests/run.json
```

Latest pointer:

```text
docs/portfolio/exports/latest.txt
```

## Lite Mode vs Full Mode

- `make portfolio` is full mode. It executes notebooks, dbt tests, DQ checks, and then exports portfolio artifacts.
- `make portfolio-validate` is lite mode. It only validates export generation from already-materialized curated outputs and is intended for fast CI checks.

## Commit Policy

Committed:
- `docs/portfolio/*.md`
- `docs/portfolio/exports/sample/**` (small, stable sample pack only)

Generated but not committed:
- `docs/portfolio/exports/<timestamp-run-id>/**`
- `docs/portfolio/exports/latest.txt`

## Related Documents

- `docs/portfolio/CUSTOMER_OVERVIEW.md`
- `docs/portfolio/TECHNICAL_OVERVIEW.md`
- `docs/portfolio/WEBSITE_EMBED.md`
