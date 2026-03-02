# Portfolio Pack

This folder contains lightweight visual artifacts generated from the Mode 1 pipeline for website and case-study embedding.

## What gets generated
Each run creates:
- `run_report.txt` and `run_report.json` (short execution summary)
- `charts/trips_volume_trend_daily.png`
- `charts/avg_fare_per_trip_daily.png`
- `charts/dq_violations_summary.png`
- `lineage/dbt_lineage_proof.png` (when dbt manifest is available)

All outputs are written under:
- `docs/portfolio/exports/<run_id>/...`

And the latest run pointer is updated at:
- `docs/portfolio/exports/latest.txt`

## Regenerate
```bash
make portfolio
```

With explicit run date for reproducibility:
```bash
make portfolio RUN_DATE=2024-01-01
```

Clean generated portfolio artifacts:
```bash
make portfolio-clean
```

## Website Embed Snippet
Use the latest exported run id:

```bash
RUN_ID=$(cat docs/portfolio/exports/latest.txt)
echo "docs/portfolio/exports/${RUN_ID}/charts/trips_volume_trend_daily.png"
echo "docs/portfolio/exports/${RUN_ID}/charts/avg_fare_per_trip_daily.png"
echo "docs/portfolio/exports/${RUN_ID}/charts/dq_violations_summary.png"
```

Suggested chart descriptions:
- `trips_volume_trend_daily.png`: Daily trip volume with a 7-day moving average.
- `avg_fare_per_trip_daily.png`: Daily average fare per trip with a 7-day moving average.
- `dq_violations_summary.png`: Data quality rule outcomes (evaluated, failed, passed).
- `dbt_lineage_proof.png`: Compact lineage proof of dbt staging-to-marts dependencies.

## Notes
- Generated files in `docs/portfolio/exports/` are intentionally excluded from git tracking (except `.gitkeep`).
- If no native trips fact table exists, trip/fare charts are generated from a deterministic proxy derived from existing Mode 1 data.
