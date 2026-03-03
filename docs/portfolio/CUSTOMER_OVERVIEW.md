# Customer Overview

## What Problem This Platform Solves

Industrial teams often have data spread across disconnected systems, which makes it hard to answer simple questions quickly:
- Are we processing more or fewer records than expected?
- Is data quality improving or drifting?
- Are pipeline SLAs being met?
- Are business KPIs moving in the right direction?

This platform creates a reliable data foundation so operations, analytics, and leadership can use the same trusted metrics and make faster decisions.

## What Data Is Extracted

In this portfolio implementation, the platform extracts two batch datasets from a public source (`fivethirtyeight`) to demonstrate production patterns:
- `recent_grads`: record-level outcomes by major (employment and salary fields).
- `bechdel_movies`: movie-level records with release year, quality dimensions, and financial fields.

Granularity:
- One row per major (`recent_grads`).
- One row per movie (`bechdel_movies`).

Frequency:
- Batch execution, typically daily in portfolio/demo mode.
- Re-runs are deterministic and generate repeatable artifacts.

## What Transformations Happen

After ingestion, data is transformed in layers:
- Cleaning: standardizes data types and required fields.
- Deduplication: removes duplicate keys (for example, repeated IDs).
- Standardization: normalizes naming conventions and categorical values.
- Enrichment: adds ingestion metadata and curated KPI-ready fields.

The pipeline uses quality checks before publishing results, so downstream charts and reports are based on validated datasets.

## What Outputs Are Produced

Customers receive:
- Curated KPI tables in the gold layer.
- Deterministic chart exports for website/case-study use.
- Data quality summaries with checked rules and violations.
- Pipeline freshness/latency SLA summaries.
- Machine-readable run manifest (`run.json`) for governance and traceability.

These outputs are optimized for executive reporting, sales conversations, and solution validation.

## Why It Matters

Business impact includes:
- Cost reduction: less manual reconciliation and fewer one-off spreadsheet workflows.
- Reliability: repeatable runs with explicit quality gates and run manifests.
- Compliance readiness: clear lineage of what was extracted, transformed, and delivered.
- Better visibility: shared KPIs and trends across technical and non-technical teams.

## 30/60/90 Day Delivery Plan

### First 30 Days

- Connect priority source systems and validate schema contracts.
- Establish baseline pipeline run and quality checks.
- Deliver first operational dashboard and exported portfolio charts.

### By 60 Days

- Expand transformations and business KPI coverage.
- Introduce SLA tracking for runtime/freshness and alerting thresholds.
- Harden reproducibility, run manifests, and documentation for auditability.

### By 90 Days

- Productionize orchestration and monitoring patterns.
- Operationalize stakeholder reporting cadence (weekly/monthly reviews).
- Finalize handoff playbooks: runbooks, quality policies, and change management process.
