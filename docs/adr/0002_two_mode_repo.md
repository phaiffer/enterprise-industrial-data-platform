# ADR 0002: Adopt Two-Mode Repository Structure

## Status
Accepted

## Context
The repository needs to satisfy two competing requirements:
1. Fast, reproducible, low-friction local execution for interviews and CI.
2. Demonstrable enterprise platform breadth (orchestration, streaming/storage infra, observability).

A single heavy execution path harms onboarding and CI speed, while a local-only path under-represents platform engineering capabilities.

## Decision
Introduce explicit execution modes:
- **Mode 1 (default)**: notebook-first local lakehouse using DuckDB + dbt + Great Expectations.
- **Mode 2 (optional)**: docker-compose enterprise stack for infra/orchestration demonstrations.

## Boundaries
- CI executes only Mode 1.
- Mode 2 is optional and independently runnable.
- Raw data remains runtime-ingested and uncommitted in both modes.

## Consequences
Positive:
- Clear portfolio narrative by audience (analytics engineering vs. platform engineering).
- Faster CI and easier contributor onboarding.
- Optional enterprise stack for depth demonstrations.

Tradeoffs:
- Additional docs and lifecycle management for two workflows.
- Need to keep mode contracts explicit to avoid drift.
