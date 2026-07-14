"""
NYC Yellow Taxi - Operational Analytics
=======================================
SQL-heavy operational analytics on NYC TLC Trip Record Data (Jan-Jun 2023)
using DuckDB. All monetary values are in USD.

Pipeline:
    1. Download monthly Parquet files + zone lookup to data/ (skipped if
       already present).
    2. Build warehouse tables 'trips', 'trips_clean', 'zones' in
       nyc_taxi.duckdb.
    3. Run business-question queries (Q1-Q6B) and export results to
       output/*.csv (ready for Power BI / Tableau).

Usage:
    pip install duckdb pandas requests
    python nyc_taxi_analysis.py             # first run downloads ~350MB
    python nyc_taxi_analysis.py --refresh   # rebuild warehouse tables
"""

import os
import sys
import time

import duckdb
import requests

# ============================================================
# CONFIGURATION
# ============================================================

DB_FILE = "nyc_taxi.duckdb"
DATA_DIR = "data"
PARQUET_DIR = os.path.join(DATA_DIR, "yellow_taxi")
ZONE_FILE = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")

MONTHS = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TRIP_URLS = [f"{BASE_URL}{m}.parquet" for m in MONTHS]

# Minimum trips per zone so the revenue-per-hour ranking is not skewed
# by zones with tiny sample sizes.
MIN_TRIPS_ZONE = 1000

DOWNLOAD_RETRIES = 3
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}

con = duckdb.connect(DB_FILE)


# ============================================================
# HELPERS
# ============================================================

def timer(label, func, *args):
    start = time.time()
    result = func(*args)
    print(f"    {label}: {time.time() - start:.2f}s")
    return result


def tables():
    return {row[0] for row in con.execute("SHOW TABLES").fetchall()}


def ready():
    return {"trips", "trips_clean", "zones"}.issubset(tables())


def sql_file_array(files):
    return "[" + ", ".join(f"'{f}'" for f in files) + "]"


# ============================================================
# DOWNLOAD RAW DATA
# ============================================================

def download_file(url, path):
    """Stream url to path. The TLC CloudFront endpoint occasionally rejects
    requests (transient 403/500), so retry a few times before giving up.
    Downloads go to a temp file first so a failed transfer never leaves a
    corrupt file behind."""
    tmp = path + ".part"
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            response = requests.get(
                url, headers=HTTP_HEADERS, stream=True, timeout=300
            )
            response.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp, path)
            return
        except requests.RequestException as exc:
            if attempt == DOWNLOAD_RETRIES:
                raise
            wait = 5 * attempt
            print(f"    failed ({exc}) - retry {attempt}/{DOWNLOAD_RETRIES} "
                  f"in {wait}s")
            time.sleep(wait)


def download_parquet():
    os.makedirs(PARQUET_DIR, exist_ok=True)
    files = []
    for url in TRIP_URLS:
        name = url.split("/")[-1]
        path = os.path.join(PARQUET_DIR, name)
        files.append(path)
        if os.path.exists(path):
            print(f"Exists {name}")
            continue
        print(f"Downloading {name}")
        download_file(url, path)
        print("Done")
    return files


def download_zone():
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(ZONE_FILE):
        return
    print("Downloading taxi zones...")
    download_file(ZONE_URL, ZONE_FILE)


# ============================================================
# SANITY CHECK
# ============================================================

def schema_check(sample_file):
    print("\n=== Schema ===")
    df = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{sample_file}')"
    ).df()
    print(df)
    return df


def row_check(files):
    print("\n=== Row Count ===")
    query = f"""
        SELECT filename, COUNT(*) AS "rows"
        FROM read_parquet({sql_file_array(files)}, filename=true)
        GROUP BY filename
        ORDER BY filename
    """
    df = con.execute(query).df()
    print(df)
    return df


# ============================================================
# CREATE WAREHOUSE TABLES
# ============================================================

def create_tables(files):
    print("\n=== Creating warehouse ===")

    # Base table with basic data-quality filters. Duration anomalies are
    # kept here on purpose: Q4 analyzes the raw trips to detect them.
    if "trips" not in tables():
        timer("trips", con.execute, f"""
            CREATE TABLE trips AS
            SELECT
                VendorID,
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                trip_distance,
                PULocationID,
                payment_type,
                fare_amount,
                tip_amount,
                total_amount,
                airport_fee,
                date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime)
                    AS trip_duration_min,
                EXTRACT(hour FROM tpep_pickup_datetime) AS pickup_hour,
                CASE
                    WHEN EXTRACT(dow FROM tpep_pickup_datetime) IN (0, 6)
                        THEN 'Weekend'
                    ELSE 'Weekday'
                END AS day_type
            FROM read_parquet({sql_file_array(files)})
            WHERE fare_amount > 0
              AND trip_distance > 0
              AND passenger_count > 0
              AND tpep_dropoff_datetime > tpep_pickup_datetime
        """)
    else:
        print("Trips exists")

    # Drop duration anomalies: > 3 sigma above the mean, or under 1 minute.
    if "trips_clean" not in tables():
        timer("trips_clean", con.execute, """
            CREATE TABLE trips_clean AS
            WITH s AS (
                SELECT
                    AVG(trip_duration_min)    AS avg_d,
                    STDDEV(trip_duration_min) AS std_d
                FROM trips
            )
            SELECT t.*
            FROM trips t, s
            WHERE trip_duration_min >= 1
              AND trip_duration_min <= avg_d + 3 * std_d
        """)
    else:
        print("Trips_clean exists")

    if "zones" not in tables():
        download_zone()
        timer("zones", con.execute, f"""
            CREATE TABLE zones AS
            SELECT * FROM read_csv_auto('{ZONE_FILE}')
        """)
    else:
        print("Zones exists")

    print("Warehouse ready")


# ============================================================
# Q1 - DEMAND PATTERN BY HOUR
# ============================================================

def q1_demand_pattern():
    print("\n=== Q1 Demand Pattern ===")
    query = """
        WITH hourly AS (
            SELECT
                pickup_hour,
                day_type,
                COUNT(*) AS total_trips
            FROM trips_clean
            GROUP BY pickup_hour, day_type
        )
        SELECT
            pickup_hour,
            day_type,
            total_trips,
            ROUND(
                AVG(total_trips) OVER (
                    PARTITION BY day_type
                    ORDER BY pickup_hour
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                ), 1
            ) AS moving_avg_3hr
        FROM hourly
        ORDER BY day_type, pickup_hour
    """
    df = con.execute(query).df()
    print(df.head(10))
    return df


# ============================================================
# Q2 - REVENUE EFFICIENCY BY ZONE
# ============================================================

def q2_revenue_zone():
    print("\n=== Q2 Revenue Zone ===")
    query = f"""
        WITH revenue AS (
            SELECT
                z.Zone    AS pickup_zone,
                z.Borough AS borough,
                COUNT(*)  AS total_trips,
                ROUND(SUM(t.total_amount), 2) AS total_revenue_usd,
                ROUND(
                    SUM(t.total_amount)
                    / NULLIF(SUM(t.trip_duration_min) / 60, 0), 2
                ) AS revenue_per_hour_usd
            FROM trips_clean t
            JOIN zones z ON t.PULocationID = z.LocationID
            GROUP BY z.Zone, z.Borough
        )
        SELECT
            *,
            RANK() OVER (ORDER BY revenue_per_hour_usd DESC) AS rank_efficiency
        FROM revenue
        WHERE total_trips >= {MIN_TRIPS_ZONE}
        ORDER BY revenue_per_hour_usd DESC
        LIMIT 10
    """
    df = con.execute(query).df()
    print(df)
    return df


# ============================================================
# Q3 - TIP BEHAVIOR
# ============================================================

def q3_tip_analysis():
    print("\n=== Q3 Tip Analysis ===")
    query = """
        WITH tips AS (
            SELECT
                pickup_hour,
                CASE payment_type
                    WHEN 1 THEN 'Credit Card'
                    WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No Charge'
                    WHEN 4 THEN 'Dispute'
                    ELSE 'Other'
                END AS payment_method,
                CASE WHEN fare_amount > 0
                     THEN tip_amount / fare_amount * 100
                END AS tip_pct
            FROM trips_clean
        )
        SELECT
            pickup_hour,
            payment_method,
            COUNT(*)               AS total_trips,
            ROUND(AVG(tip_pct), 2) AS avg_tip_pct
        FROM tips
        WHERE tip_pct IS NOT NULL
        GROUP BY pickup_hour, payment_method
        ORDER BY pickup_hour, payment_method
    """
    df = con.execute(query).df()
    print(df.head(15))
    return df


# ============================================================
# Q4 - ANOMALY DETECTION
# Uses raw 'trips' (not 'trips_clean') on purpose: the goal of
# this query is to surface the anomalous trips themselves.
# ============================================================

def q4_anomaly_detection():
    print("\n=== Q4 Trip Anomaly ===")
    query = """
        WITH stats AS (
            SELECT
                AVG(trip_duration_min)    AS avg_duration,
                STDDEV(trip_duration_min) AS std_duration
            FROM trips
        )
        SELECT
            VendorID,
            tpep_pickup_datetime,
            trip_distance,
            trip_duration_min,
            fare_amount
        FROM trips, stats
        WHERE trip_duration_min > avg_duration + 3 * std_duration
           OR trip_duration_min < 1
        ORDER BY trip_duration_min DESC
        LIMIT 50
    """
    df = con.execute(query).df()
    print(df)
    return df


# ============================================================
# Q5 - WEEKDAY VS WEEKEND
# ============================================================

def q5_weekday_weekend():
    print("\n=== Q5 Weekday Weekend ===")
    query = """
        SELECT
            day_type,
            COUNT(*)                         AS total_trips,
            ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
            ROUND(AVG(trip_distance), 2)     AS avg_distance_miles,
            ROUND(AVG(total_amount), 2)      AS avg_fare_usd,
            ROUND(
                AVG(total_amount)
                / NULLIF(AVG(trip_duration_min) / 60, 0), 2
            ) AS revenue_per_hour_usd
        FROM trips_clean
        GROUP BY day_type
    """
    df = con.execute(query).df()
    print(df)
    return df


# ============================================================
# Q6 - AIRPORT VS NON-AIRPORT
# ============================================================

def q6_airport_compare():
    print("\n=== Q6 Airport Comparison ===")
    query = """
        SELECT
            CASE WHEN airport_fee > 0 THEN 'Airport Trip'
                 ELSE 'Non Airport'
            END AS trip_type,
            COUNT(*)                         AS total_trips,
            ROUND(AVG(total_amount), 2)      AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2)     AS avg_distance_miles,
            ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
            ROUND(
                AVG(tip_amount / NULLIF(fare_amount, 0) * 100), 2
            ) AS avg_tip_pct
        FROM trips_clean
        GROUP BY trip_type
    """
    df = con.execute(query).df()
    print(df)
    return df


# ============================================================
# Q6B - AIRPORT ZONE DETAIL
# ============================================================

def q6_airport_zone():
    print("\n=== Q6B Airport Zone ===")
    query = """
        SELECT
            z.Zone   AS pickup_zone,
            COUNT(*) AS total_trips,
            ROUND(AVG(t.total_amount), 2) AS avg_fare_usd,
            ROUND(
                AVG(t.tip_amount / NULLIF(t.fare_amount, 0) * 100), 2
            ) AS avg_tip_pct
        FROM trips_clean t
        JOIN zones z ON t.PULocationID = z.LocationID
        WHERE t.airport_fee > 0
        GROUP BY z.Zone
        HAVING COUNT(*) >= 100
        ORDER BY total_trips DESC
    """
    df = con.execute(query).df()
    print(df.head(20))
    return df


# ============================================================
# EXPORT RESULTS
# ============================================================

def export_results(results):
    print("\n=== Export Results ===")
    os.makedirs("output", exist_ok=True)

    for name, df in results.items():
        file = f"output/{name}.csv"
        df.to_csv(file, index=False)
        print(f"Saved: {file}")


# ============================================================
# DATABASE REFRESH
# ============================================================

def refresh_database():
    print("\n=== Refresh Database ===")
    for table in ["trips", "trips_clean", "zones"]:
        con.execute(f"DROP TABLE IF EXISTS {table}")
    print("Old tables removed.")


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    if "--refresh" in sys.argv:
        refresh_database()

    files = download_parquet()

    if not ready():
        schema_check(files[0])
        row_check(files)
        create_tables(files)
    else:
        print("\nDatabase sudah siap.")

    results = {
        "q1_demand_pattern": q1_demand_pattern(),
        "q2_revenue_zone": q2_revenue_zone(),
        "q3_tip_analysis": q3_tip_analysis(),
        "q4_anomaly_detection": q4_anomaly_detection(),
        "q5_weekday_weekend": q5_weekday_weekend(),
        "q6_airport_compare": q6_airport_compare(),
        "q6_airport_zone": q6_airport_zone(),
    }

    export_results(results)

    print("""
================================================
NYC TAXI ANALYTICS COMPLETE

Output   : output/*.csv
Database : nyc_taxi.duckdb
Ready for: Power BI, Tableau, further SQL analysis
================================================
    """)


if __name__ == "__main__":
    main()
