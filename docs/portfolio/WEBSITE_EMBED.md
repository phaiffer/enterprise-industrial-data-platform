# Website Embed Guide

## Resolve Latest Run

```bash
RUN_ID=$(cat docs/portfolio/exports/latest.txt)
echo "docs/portfolio/exports/${RUN_ID}/charts/throughput_trend_daily.png"
echo "docs/portfolio/exports/${RUN_ID}/charts/quality_violations_by_category.png"
echo "docs/portfolio/exports/${RUN_ID}/charts/freshness_latency_sla_compliance.png"
echo "docs/portfolio/exports/${RUN_ID}/charts/business_kpi_pass_rate_trend.png"
echo "docs/portfolio/exports/${RUN_ID}/manifests/run.json"
```

## Copy/Paste Markdown Snippets

```markdown
![Throughput trend](docs/portfolio/exports/<RUN_ID>/charts/throughput_trend_daily.png)
![Quality violations](docs/portfolio/exports/<RUN_ID>/charts/quality_violations_by_category.png)
![Freshness and latency SLA](docs/portfolio/exports/<RUN_ID>/charts/freshness_latency_sla_compliance.png)
![Business KPI trend](docs/portfolio/exports/<RUN_ID>/charts/business_kpi_pass_rate_trend.png)
```

## Suggested Captions

- `throughput_trend_daily.png`: Daily records processed with a 7-day smoothing line to show sustained throughput direction.
- `quality_violations_by_category.png`: Quality rules checked versus violations by rule category, highlighting trust in downstream data.
- `freshness_latency_sla_compliance.png`: Pipeline stage latency against SLA threshold, showing operational reliability.
- `business_kpi_pass_rate_trend.png`: Long-term business KPI trend with rolling average to separate signal from short-term variation.

## Short Case Study (200-300 words)

This portfolio project demonstrates how an enterprise data platform can move from fragmented, hard-to-trust data to repeatable decision support in a short timeline. The implementation uses a layered lakehouse model (bronze, silver, gold) with dbt for transformations and Great Expectations for quality validation. While the portfolio dataset is public, the operational pattern mirrors real industrial deployments where teams need predictable refresh cycles, quality transparency, and stakeholder-ready reporting.

The portfolio pack introduces deterministic exports for sales and customer conversations. Every run writes a dedicated folder with chart assets, a lightweight report, and a machine-readable manifest containing run metadata, git SHA, source names, row counts by stage, dataset time range, and quality outcomes. This allows technical teams to verify reproducibility while giving customer-facing teams an easy way to embed consistent visuals in websites, proposals, and case studies.

The result is practical alignment between engineering rigor and business communication. Operations teams get throughput and SLA views, data teams get explicit quality signals, and leadership gets KPI trends tied to clear provenance. Instead of preparing one-off screenshots, the organization can regenerate the same narrative from current validated outputs with a single command (`make portfolio`), reducing manual effort and improving confidence during customer discussions.

## Deep Dive

- Stack:
  - Python, Papermill, dbt (DuckDB), Great Expectations, Matplotlib, Make, GitHub Actions.
- Patterns:
  - Medallion layering, deterministic run folders, latest pointer indirection, machine-readable run manifest.
- Quality gates:
  - dbt tests, Great Expectations checkpoint, image size limits, CI validation target (`make portfolio-validate`).
