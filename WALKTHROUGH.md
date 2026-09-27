# Walkthrough: building this pipeline from zero

This is the full build story, in the order it actually happened — including the dead ends, so the reasoning is visible, not just the final result.

## 1. Choosing the scenario and dataset

Started with a US flight on-time performance dataset (BTS `T_ONTIME_REPORTING`-style) and framed the business scenario as **Airline Operational Performance & Delay Analytics** — an airline/airport authority tracking every flight leg to identify which carriers, routes, and time slots drive delays and cancellations.

## 2. Landing the first source

The flight CSV was manually uploaded into a Unity Catalog volume (`dwbi.stage`). In `01_DWBI_bronze.ipynb`:

```python
df_flights = spark.read.option("header", True).csv("/Volumes/dwbi/stage/volume/ontime_reporting.csv")
df_flights.write.format("delta").mode("overwrite").saveAsTable("dwbi.bronze.ontime_reporting")
```

## 3. Adding a second source type — airport lookup (JSON)

To satisfy the multi-source-type requirement (and to enrich `ORIGIN`/`DEST` codes with real names and coordinates), the [`mwgg/Airports`](https://github.com/mwgg/Airports) public JSON dataset was fetched:

```python
import requests, json

url = "https://raw.githubusercontent.com/mwgg/Airports/master/airports.json"
airports_data = requests.get(url).json()
```

The raw file turned out to be a JSON *object* keyed by ICAO code, not an array — `spark.read.json()` can't parse that directly. It was flattened in Python first, then converted to a Spark DataFrame:

```python
airport_list = list(airports_data.values())
df_airports = spark.createDataFrame(airport_list)
df_airports.write.format("delta").mode("overwrite").saveAsTable("dwbi.bronze.airports_lookup")
```

## 4. Building the silver layer

In `02_DWBI_silver.ipynb`, the flight data was joined to **two aliased copies** of the airport lookup — one for `ORIGIN`, one for `DEST` — since the same lookup table needed to be joined twice with different column names to avoid ambiguity:

```python
airport_origin = df_airports.select(F.col("iata").alias("origin_iata"), ...)
airport_dest = df_airports.select(F.col("iata").alias("dest_iata"), ...)

df_enriched = df_flights \
    .join(airport_origin, df_flights.ORIGIN == airport_origin.origin_iata, "left") \
    .join(airport_dest, df_flights.DEST == airport_dest.dest_iata, "left")
```

**Null investigation:** a null count across all columns showed `DEP_TIME`, `DEP_DELAY`, `ARR_TIME`, `ARR_DELAY`, and `ACTUAL_ELAPSED_TIME` with 19,800–21,900 nulls each, and `CANCELLATION_CODE` with 526,882 nulls. Cross-checking against `CANCELLED`/`DIVERTED` confirmed these were structural, not random — see `TECHNIQUES.md` for the full reasoning. Handling applied:

```python
df_silver = df_silver.withColumn(
    "is_delay_applicable",
    (F.col("CANCELLED").cast("double") == 0) & (F.col("DIVERTED").cast("double") == 0)
)
df_silver = df_silver.withColumn(
    "CANCELLATION_CODE",
    F.when(F.col("CANCELLATION_CODE").isNull(), "N").otherwise(F.col("CANCELLATION_CODE"))
)
```

**Type casting:** the CSV had been read with no explicit schema, so every column — including numeric ones — was a string. `FL_DATE` parsing initially failed with the format `"M/d/yyyy H:mm"`; the actual data included seconds and AM/PM, so the working format was `"M/d/yyyy h:mm:ss a"`:

```python
df_silver = df_silver \
    .withColumn("FL_DATE", F.to_date("FL_DATE", "M/d/yyyy h:mm:ss a")) \
    .withColumn("DEP_DELAY", F.col("DEP_DELAY").cast("double")) \
    .withColumn("ARR_DELAY", F.col("ARR_DELAY").cast("double")) \
    # ...and the remaining numeric columns
```

A `dep_hour` column was derived from `CRS_DEP_TIME` (still a clean zero-padded 4-digit string at this point) for later time-bucketing:

```python
df_silver = df_silver.withColumn("dep_hour", F.col("CRS_DEP_TIME").substr(1, 2).cast("int"))
```

## 5. Adding the third source — carrier name lookup (self-authored JSON)

`OP_UNIQUE_CARRIER` only carries 2-letter codes with no airline name. Rather than search for a third-party mapping, the official BTS list of CY2024 reporting carriers (15 total) was hand-authored as a JSON dict and validated against the actual distinct carrier codes in the data — zero unmatched codes. See `DATA.md` for the full list and source citation.

Silver was then persisted:
```python
df_silver.write.format("delta").mode("overwrite").saveAsTable("dwbi.silver.gold_ready")
```

## 6. Building the gold star schema

In `03_DWBI_gold.ipynb`, four dimensions were built from the silver table (`dim_date`, `dim_carrier`, `dim_airport`, `dim_time_bucket`), each with a `GENERATED ALWAYS AS IDENTITY` surrogate key, followed by the fact table joining all four:

```python
fact_stage = df_gold \
    .join(dim_date, df_gold.FL_DATE == dim_date.full_date, "left") \
    .join(dim_carrier, df_gold.OP_UNIQUE_CARRIER == dim_carrier.carrier_code, "left") \
    .join(origin_dim, df_gold.origin_iata == F.col("origin_code_match"), "left") \
    .join(dest_dim, df_gold.dest_iata == F.col("dest_code_match"), "left") \
    .join(dim_time_bucket, df_gold.dep_time_bucket == dim_time_bucket.bucket_name, "left") \
    .select(...)
```

## 7. The duplicate-row bug

After loading, a validation check showed `fact_flight_performance` had exactly **4× more rows** than the silver source table (2,189,084 vs. 547,271) — too clean a ratio to be a coincidence. The investigation ruled out `dim_airport` as the cause (334 rows, 334 distinct codes — clean), and eventually traced the real cause: the `CREATE TABLE IF NOT EXISTS` + `INSERT INTO` pattern isn't idempotent, and the fact-table cell had been re-run multiple times while debugging, appending duplicate rows each time.

**Fix:** dropped and rebuilt the gold tables from a single clean run. Post-fix, `fact_flight_performance` matched the silver source exactly at 547,271 rows. Going forward, fact and mart tables use `CREATE OR REPLACE TABLE ... AS SELECT` to make re-runs safe.

## 8. Building the data marts

Two audience-specific marts were built on top of the validated gold schema:

- `route_performance` — aggregated to origin + destination + carrier grain, for Network Planning
- `airport_operations` — aggregated to airport + date grain (combining each airport's departure-side and arrival-side traffic via a full outer join), for Airport Ground Operations

Both use `CREATE OR REPLACE TABLE` from the start, applying the lesson from step 7.

## 9. Exporting for Power BI

No live Databricks-to-Power-BI connector was available in this environment, so the gold tables were exported manually. Spark's native CSV writer splits output into a multi-part folder, which is awkward to download by hand — `toPandas()` was used instead to produce a single clean file per table (safe here since the largest table is ~550K rows):

```python
export_config = {
    "dwbi.gold.dim_date": "dim_date",
    "dwbi.gold.dim_carrier": "dim_carrier",
    "dwbi.gold.dim_airport": "dim_airport",
    "dwbi.gold.dim_time_bucket": "dim_time_bucket",
    "dwbi.gold.fact_flight_performance": "fact_flight_performance"
}

for table_name, file_name in export_config.items():
    pdf = spark.table(table_name).toPandas()
    pdf.to_csv(f"/Volumes/dwbi/stage/volume/powerbi_export/{file_name}.csv", index=False)
```

The five CSVs were then downloaded from the Unity Catalog volume (Catalog Explorer → Volumes → download) and imported into Power BI Desktop via **Get Data → Text/CSV**, one file at a time. `dim_airport` was duplicated in Power BI's model view (as `dim_airport_dest`) to recreate the role-playing relationship, and all five relationships were wired to the fact table to complete the star schema in the model.

From here, dashboard pages (Executive Summary, Trend Analysis, Interactive Analysis) were built on top of this model.
