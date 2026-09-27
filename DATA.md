# Data sources

This project deliberately combines **three sources in two different formats** — one CSV and two JSON — to satisfy the assignment's requirement to integrate data from multiple operational systems into a unified analytical environment.

## 1. Flight on-time performance data (CSV)

**Table:** `dwbi.bronze.ontime_reporting`
**Format:** CSV, single file, manually uploaded into a Unity Catalog volume
**Source type:** OLTP-style operational extract, in the style of the US Bureau of Transportation Statistics' `T_ONTIME_REPORTING` dataset
**Scope:** 547,271 flight records, covering 31 distinct calendar days (January 2024) across 15 US air carriers

**Columns:**

| Column | Description |
|---|---|
| `YEAR`, `FL_DATE` | Flight date |
| `OP_UNIQUE_CARRIER`, `OP_CARRIER_AIRLINE_ID`, `OP_CARRIER_FL_NUM` | Operating carrier and flight number |
| `ORIGIN_AIRPORT_ID`, `ORIGIN`, `ORIGIN_CITY_NAME`, `ORIGIN_STATE_ABR` | Origin airport |
| `DEST_AIRPORT_ID`, `DEST`, `DEST_CITY_NAME`, `DEST_STATE_ABR` | Destination airport |
| `CRS_DEP_TIME`, `DEP_TIME`, `DEP_DELAY` | Scheduled vs. actual departure |
| `CRS_ARR_TIME`, `ARR_TIME`, `ARR_DELAY` | Scheduled vs. actual arrival |
| `CANCELLED`, `CANCELLATION_CODE`, `DIVERTED` | Outcome flags |
| `ACTUAL_ELAPSED_TIME`, `DISTANCE` | Flight duration and distance (miles) |

**Extraction method:** manual upload into a Databricks Unity Catalog volume, then read via `spark.read.option("header", True).csv(...)` and persisted as a bronze Delta table with `.saveAsTable()`.

**Known data quality characteristic:** `DEP_TIME`, `DEP_DELAY`, `ARR_TIME`, `ARR_DELAY`, and `ACTUAL_ELAPSED_TIME` are null for a meaningful subset of rows (~19,800–21,900 depending on the column). These are **not random missing values** — they are structurally null because the flight was cancelled or diverted and never actually recorded an actual time. This distinction drove the null-handling approach documented below and in `TECHNIQUES.md`.

## 2. Airport reference lookup (JSON)

**Table:** `dwbi.bronze.airports_lookup`
**Format:** JSON, fetched from a public GitHub-hosted dataset
**Source:** [`mwgg/Airports`](https://github.com/mwgg/Airports) — a community-maintained, freely available dataset of ~29,000 airports worldwide
**Raw file:** `https://raw.githubusercontent.com/mwgg/Airports/master/airports.json`

**Why this source:** the flight dataset only carries 3-letter airport codes (`ORIGIN`/`DEST`) and city names — no coordinates, no full airport names beyond the city. This lookup enriches both with `name`, `city`, `country`, `iata`, `icao`, `latitude`, `longitude`, `altitude`, and `tz`.

**Structural challenge:** the raw JSON is not a flat array — it's a single JSON *object* keyed by ICAO code (`{"KJFK": {...}, "EGLL": {...}, ...}`), so it cannot be read directly with `spark.read.json()`. It was fetched with `requests`, parsed with Python's `json` module, flattened with `list(airports_data.values())`, and only then converted into a Spark DataFrame with `spark.createDataFrame()`.

**Scale after ingestion:** 29,307 total rows, but only 7,918 distinct `iata` codes — most entries in the dataset are small airfields/heliports with no assigned IATA code (empty string), which don't affect this project since only the 334 airports actually used as an `ORIGIN`/`DEST` in the flight data end up in the final dimension table.

**Join key:** `iata` ↔ `ORIGIN` / `DEST` (not `icao`, which the flight dataset doesn't use).

## 3. Carrier name lookup (JSON, self-authored)

**Table:** `dwbi.bronze.carrier_lookup`
**Format:** JSON, hand-authored (not fetched from an external file)
**Source:** [BTS Technical Reporting Directive #38](https://www.bts.gov/explore-topics-and-geography/modes/aviation/number-38-technical-reporting-directive-reporting-air) — the official list of 14 mandatory-reporting US air carriers for calendar year 2024, plus Endeavor Air as a voluntary reporter (15 carriers total)

**Why self-authored rather than fetched:** the flight dataset only carries 2-letter carrier codes (`OP_UNIQUE_CARRIER`, e.g. `9E`, `AA`, `DL`) with no full airline name. Since the set of CY2024 reporting carriers is small, fixed, and officially published by BTS, a hand-curated JSON lookup was more accurate and auditable than searching for a third-party mapping.

```json
{
  "AA": "American Airlines",
  "AS": "Alaska Airlines",
  "B6": "JetBlue Airways",
  "DL": "Delta Air Lines",
  "F9": "Frontier Airlines",
  "G4": "Allegiant Air",
  "HA": "Hawaiian Airlines",
  "MQ": "Envoy Air",
  "NK": "Spirit Airlines",
  "OH": "PSA Airlines",
  "OO": "SkyWest Airlines",
  "UA": "United Airlines",
  "WN": "Southwest Airlines",
  "YX": "Republic Airways",
  "9E": "Endeavor Air"
}
```

**Validation:** every distinct `OP_UNIQUE_CARRIER` code present in the flight data was checked against this lookup — zero unmatched codes, confirming full coverage.

**Join key:** `carrier_code` ↔ `OP_UNIQUE_CARRIER`.

## How the sources relate

```
ontime_reporting.ORIGIN  ──┐
ontime_reporting.DEST    ──┼──> airports_lookup.iata
ontime_reporting.OP_UNIQUE_CARRIER ──> carrier_lookup.carrier_code
```

All joins are left joins from the flight fact data outward, since the flight table is the transactional core and the two JSON sources are pure reference/lookup data.
