# Airline Operational Performance & Delay Analytics

An end-to-end Data Warehousing & Business Intelligence solution built on **Databricks** (Unity Catalog, Delta Lake, PySpark) with a **Power BI** presentation layer, developed for the IT3101 Data Warehousing & BI module assignment (BSc (Hons) in Computing, Faculty of Computing).

## Business scenario

The project models an airline operations analytics system: tracking every scheduled flight leg (route, carrier, schedule vs. actual times, delays, cancellations, diversions) to answer questions a Network Planning or Airport Operations team would actually ask — *which routes chronically underperform, which carriers have the worst on-time record, and which airports have the roughest operational days.*

Domain: **Transportation / Aviation**, based on a real-world US flight on-time performance dataset in the style of the Bureau of Transportation Statistics (BTS) `T_ONTIME_REPORTING` table.

## Architecture

Four layers, following a medallion (bronze/silver/gold) design:

```
Data sources (CSV + 2× JSON)
        ↓
Data integration (PySpark ETL, Databricks notebooks)
        ↓
Storage (Unity Catalog: bronze → silver → gold → data marts)
        ↓
Presentation (Power BI, manual CSV import)
```

See [`DATA.md`](./DATA.md) for the sources, [`TECHNIQUES.md`](./TECHNIQUES.md) for the tools and design patterns, and [`WALKTHROUGH.md`](./WALKTHROUGH.md) for the full build process from zero.

## Repository structure

```
notebooks/
  01_DWBI_bronze.ipynb    # Extract & load raw sources into Unity Catalog
  02_DWBI_silver.ipynb    # Clean, enrich, handle nulls, cast types
  03_DWBI_gold.ipynb      # Build the dimensional star schema
docs/
  DATA.md                 # Data sources in detail
  TECHNIQUES.md           # Tools and techniques used
  WALKTHROUGH.md          # Step-by-step build narrative
powerbi/
  *.pbix                  # Power BI dashboard file
README.md
```

## Data warehouse design

**Star schema**, grain = one row per flight leg:

- **Fact:** `fact_flight_performance` — departure/arrival delay, elapsed time, distance, cancellation/diversion flags
- **Dimensions:** `dim_date`, `dim_carrier`, `dim_airport` (role-playing: origin + destination), `dim_time_bucket`
- **Data marts:** `route_performance` (origin–destination–carrier grain, for Network Planning), `airport_operations` (airport–date grain, for Airport Ground Operations)

## Tech stack

| Layer | Tool |
|---|---|
| Storage & compute | Databricks (Unity Catalog, Delta Lake) |
| Transformation | PySpark, Spark SQL |
| Dashboarding | Power BI Desktop |

## Reproducing this project

1. Upload the flight on-time performance CSV to a Unity Catalog volume.
2. Run `01_DWBI_bronze.ipynb` to land the CSV and the two JSON reference datasets as bronze Delta tables.
3. Run `02_DWBI_silver.ipynb` to clean, enrich, and type-cast the data.
4. Run `03_DWBI_gold.ipynb` to build the star schema and data marts.
5. Export the gold tables to CSV (see [`WALKTHROUGH.md`](./WALKTHROUGH.md#exporting-for-power-bi)) and import them into Power BI Desktop.
