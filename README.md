# NYC Yellow Taxi — Operational Analytics

SQL-heavy operational analytics on **19.5 million taxi trips** (NYC TLC Trip Record Data, Jan–Jun 2023), built with **DuckDB** as a local analytical warehouse. The pipeline downloads raw Parquet files, builds cleaned warehouse tables, answers six operational business questions in pure SQL, and exports BI-ready CSVs.

All monetary values are in USD.

## Key Insights

| # | Finding |
|---|---------|
| 1 | **Weekday demand is double-peaked**: it climbs steeply from ~38K trips at 4 AM to **624K trips at 9 AM** — a 16× swing that fleet allocation must absorb every morning. |
| 2 | **LaGuardia Airport is the volume king** (630K pickups, **$41.7M revenue** in 6 months), but smaller Queens zones like Flushing Meadows–Corona Park earn more per driving hour ($207/hr vs LGA's $135/hr). |
| 3 | **Credit-card riders tip ~24–25%** consistently across all hours. Cash tips show as 0% — they are simply not recorded by the meter, a critical caveat for any tipping analysis on this dataset. |
| 4 | **Data quality matters**: the raw data contains trips "lasting" up to **117 hours** with ordinary fares (meter errors / unclosed trips). These are detected via 3-sigma thresholding and excluded from all revenue metrics. |
| 5 | **Weekends are slightly more efficient for drivers**: $111.63 revenue/hour vs $106.41 on weekdays, with shorter average trip durations (14.9 vs 16.2 min). |
| 6 | **Airport trips pay 3× the fare** ($74.62 vs $24.10 average) over 5× the distance — but riders tip a *lower* percentage (16.8% vs 21.3%). JFK leads on volume and fare; LaGuardia riders tip better (20.8% vs 13.7%). |

## Business Questions

| Query | Question | SQL techniques |
|-------|----------|----------------|
| Q1 | How does demand fluctuate by hour, weekday vs weekend? | Window function (3-hour moving average), date bucketing |
| Q2 | Which pickup zones generate the most revenue per driving hour? | JOIN to lookup table, `RANK()`, sample-size guardrail (≥1,000 trips) |
| Q3 | How does tipping behave across hours and payment methods? | Layered CTEs, `CASE WHEN` |
| Q4 | Which trips are duration anomalies (data quality)? | Statistical thresholding (3-sigma) on raw data |
| Q5 | Weekday vs weekend: which is operationally more efficient? | Aggregation with derived efficiency metrics |
| Q6 / Q6B | How do airport trips differ, and which airport zones perform best? | `CASE WHEN` segmentation on `airport_fee`, `HAVING` filter |

## How It Works

```
TLC CloudFront (Parquet, ~350MB)
        │  download once, with retry (endpoint intermittently returns 403/500)
        ▼
data/yellow_taxi/*.parquet
        │  materialize with column pruning + data-quality filters
        ▼
nyc_taxi.duckdb ── trips        (basic quality filters applied)
                ── trips_clean  (18.2M rows, duration anomalies removed via 3-sigma)
                ── zones        (LocationID → zone/borough lookup)
        │  Q1–Q6B in pure SQL
        ▼
output/*.csv  →  Power BI / Tableau
```

**Cleaning methodology.** The base `trips` table drops rows with non-positive fares, distances, or passenger counts, and dropoffs before pickups. `trips_clean` additionally removes trips shorter than 1 minute or longer than 3 standard deviations above the mean duration. From 19.5M raw rows, 18.2M survive both stages. Q4 intentionally queries the *raw* table — its job is to surface those anomalies.

Everything is **idempotent**: downloaded files and built tables are skipped on re-runs, so the full pipeline finishes in seconds after the first run.

## Getting Started

```bash
pip install duckdb pandas requests
python nyc_taxi_analysis.py             # first run downloads ~350MB of Parquet
python nyc_taxi_analysis.py --refresh   # rebuild warehouse tables from local data
```

## Project Structure

```
├── nyc_taxi_analysis.py    # the whole pipeline: download → warehouse → queries → export
├── data/                   # raw Parquet + zone lookup (gitignored)
├── output/                 # query results as CSV (gitignored)
└── nyc_taxi.duckdb         # DuckDB warehouse file (gitignored)
```

## Tech Stack

- **DuckDB** — in-process analytical database; queries Parquet directly and persists the warehouse to a single file
- **Python** (requests, pandas) — download orchestration with retry, CSV export
- **SQL** — window functions, CTEs, statistical filtering, ranking

## Data Source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Yellow Taxi, January–June 2023 (~19.5M trips), plus the official taxi zone lookup table.
