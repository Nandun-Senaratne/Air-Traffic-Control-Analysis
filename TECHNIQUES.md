# Techniques & tools

## Platform

**Databricks** (Free/Community-tier workspace) with **Unity Catalog** for governance and **Delta Lake** as the table format throughout — bronze, silver, gold, and mart tables are all Delta tables, giving ACID writes and schema enforcement for free.

## Medallion architecture

The pipeline follows the standard bronze → silver → gold pattern:

- **Bronze** — raw ingested data, one table per source, minimal transformation (just schema/format landing)
- **Silver** — cleaned, joined, type-cast, enriched, with documented null-handling decisions
- **Gold** — a proper dimensional star schema, ready for BI consumption
- **Data marts** — pre-aggregated, audience-specific views built on top of gold

Kept in three separate notebooks (`01_DWBI_bronze`, `02_DWBI_silver`, `03_DWBI_gold`) so each layer's transformations are independently re-runnable and reviewable.

## Dimensional modelling (Kimball approach)

- **Star schema**, not snowflake — one fact table (`fact_flight_performance`) surrounded by four dimensions, each with a `GENERATED ALWAYS AS IDENTITY` surrogate key.
- **Role-playing dimension pattern** for `dim_airport`: a single physical airport dimension table is referenced twice from the fact table (`origin_airport_key`, `dest_airport_key`), rather than maintaining two separate airport tables. This avoids duplicating airport attributes and keeps the dimension a single source of truth.
- **Grain** is explicit: one fact row = one scheduled flight leg.

## Null-handling strategy

A key modelling decision was distinguishing **structural nulls** from **missing data**:

- `DEP_TIME`, `DEP_DELAY`, `ARR_TIME`, `ARR_DELAY`, `ACTUAL_ELAPSED_TIME` are null specifically for cancelled/diverted flights — these are not Missing Completely at Random (MCAR), they're a direct consequence of the flight never actually operating as scheduled.
- **Decision:** these were left as `NULL` rather than imputed (mean/zero-fill would have artificially deflated delay averages), and a derived boolean flag `is_delay_applicable` (`CANCELLED == 0 AND DIVERTED == 0`) was added so downstream aggregations can filter cleanly without dropping rows.
- `CANCELLATION_CODE`, a categorical attribute, was handled differently: nulls (non-cancelled flights) were replaced with the sentinel value `"N"` rather than left null, since BI tools render true nulls as ambiguous blank categories in filters/slicers — an explicit "not applicable" member is the standard Kimball approach for dimension-like attributes.

## Idempotent table loads

An important lesson learned mid-project: the initial gold-layer load pattern —

```sql
CREATE TABLE IF NOT EXISTS ...;
INSERT INTO ... SELECT * FROM ...;
```

— is **not idempotent**. Re-running a cell during debugging (common when iterating on a notebook) causes `INSERT INTO` to append duplicate rows every time, since `CREATE TABLE IF NOT EXISTS` is a no-op once the table exists. This caused an exact 4× row inflation in `fact_flight_performance` that was initially mistaken for a dimension join fan-out bug, before being traced to repeated cell execution via a `distinct()` vs `count()` comparison.

**Fix going forward:** `CREATE OR REPLACE TABLE ... AS SELECT` (CTAS) is used for fact and mart tables, since re-running it always produces a fresh, correct table regardless of how many times the cell executes.

## Data mart design

Two department-specific marts sit on top of the gold star schema, each with a different **grain** chosen to match its audience's actual decision-making unit rather than reusing the flight-level fact grain:

| Mart | Grain | Audience | Answers |
|---|---|---|---|
| `route_performance` | origin + destination + carrier | Network Planning / Scheduling | "Which routes/carriers chronically underperform?" |
| `airport_operations` | airport + date | Airport Ground Operations | "Which airport had a rough operational day, and was it a departure-side or arrival-side problem?" |

## Debugging methodology

The recurring diagnostic pattern used throughout: whenever a row count looked suspicious, compare `.count()` against `.select(<candidate key columns>).distinct().count()` on both the suspected dimension and the fact table. An exact, clean multiplicative ratio (e.g. precisely 4×) across an entire dataset is a strong signal of a **systematic** issue (like a duplicated table or repeated INSERT) rather than a **localized** data-quality issue (like a few airports having duplicate reference records) — the latter would produce a messier, non-uniform ratio.

## Presentation layer

**Power BI Desktop**, connected via **manual CSV export/import** rather than a live connector (no direct Databricks-to-Power-BI connection was available in this environment). Gold-layer tables were exported with `.toPandas().to_csv()` (rather than Spark's native distributed CSV writer, which splits output into multiple partition files) to produce single, clean CSV files suitable for manual download and import. The role-playing `dim_airport` dimension was recreated in Power BI's model view as a duplicated table (`dim_airport_dest`) to support the two independent relationships to the fact table.
