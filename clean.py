import duckdb
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)
def clean_parquet():
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logging.info("Connected to emissions database")
        print("Connected to emissions database")
        con.execute("PRAGMA memory_limit='4GB'")

        # clean yellow_taxi
        con.execute("""
            CREATE OR REPLACE TABLE yellow_taxi AS
            SELECT DISTINCT
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance
            FROM yellow_taxi
            WHERE passenger_count > 0
            AND trip_distance > 0
            AND trip_distance <= 100
            AND (tpep_dropoff_datetime - tpep_pickup_datetime) > INTERVAL '0' SECOND
            AND (tpep_dropoff_datetime - tpep_pickup_datetime) <= INTERVAL '24' HOUR
            AND EXTRACT(year FROM tpep_pickup_datetime) BETWEEN 2015 AND 2024
            AND EXTRACT(year FROM tpep_dropoff_datetime) BETWEEN 2015 AND 2024
        """)
        logging.info("Cleaned yellow_taxi table")
        print("Cleaned yellow_taxi table")

        # TEST: verify above conditions no longer exist in the data
        yellow_zero_passengers = con.execute("""
            SELECT COUNT(*) FROM yellow_taxi WHERE passenger_count = 0;
        """).fetchone()[0]
        yellow_zero_distance = con.execute("""
            SELECT COUNT(*) FROM yellow_taxi WHERE trip_distance = 0;
        """).fetchone()[0]
        yellow_long_distance = con.execute("""
            SELECT COUNT(*) FROM yellow_taxi WHERE trip_distance > 100;
        """).fetchone()[0]
        yellow_long_duration = con.execute("""
            SELECT COUNT(*) FROM yellow_taxi
            WHERE (tpep_dropoff_datetime - tpep_pickup_datetime) > INTERVAL '24' HOUR;
        """).fetchone()[0]
        y_min_year, y_max_year = con.execute("""
            SELECT MIN(EXTRACT(year FROM tpep_pickup_datetime)), MAX(EXTRACT(year FROM tpep_pickup_datetime))
            FROM yellow_taxi;
        """).fetchone()
        logging.info(f"Yellow Taxi - Zero Passengers: {yellow_zero_passengers}, Zero Distance: {yellow_zero_distance}, Long Distance: {yellow_long_distance}, Long Duration: {yellow_long_duration}, Year Range: {y_min_year} - {y_max_year}")
        print(f"Yellow Taxi - Zero Passengers: {yellow_zero_passengers}, Zero Distance: {yellow_zero_distance}, Long Distance: {yellow_long_distance}, Long Duration: {yellow_long_duration}, Year Range: {y_min_year} - {y_max_year}")
    except Exception as e:
        logging.error(f"Error cleaning parquet files: {e}")

    try:
        # clean green taxi
        con.execute("""
            CREATE OR REPLACE TABLE green_taxi AS
            SELECT DISTINCT
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            passenger_count,
            trip_distance
            FROM green_taxi
            WHERE passenger_count > 0
            AND trip_distance > 0
            AND trip_distance <= 100
            AND (lpep_dropoff_datetime - lpep_pickup_datetime) > INTERVAL '0' SECOND
            AND (lpep_dropoff_datetime - lpep_pickup_datetime) <= INTERVAL '24' HOUR
            AND EXTRACT(year FROM lpep_pickup_datetime) BETWEEN 2015 AND 2024
            AND EXTRACT(year FROM lpep_dropoff_datetime) BETWEEN 2015 AND 2024
        """)
        logging.info("Cleaned green taxi table")
        print("Cleaned green taxi table")

        # TEST: verify above conditions no longer exist in the data
        green_zero_passengers = con.execute("""
            SELECT COUNT(*) FROM green_taxi WHERE passenger_count = 0;
        """).fetchone()[0]
        green_zero_distance = con.execute("""
            SELECT COUNT(*) FROM green_taxi WHERE trip_distance = 0;
        """).fetchone()[0]
        green_long_distance = con.execute("""
            SELECT COUNT(*) FROM green_taxi WHERE trip_distance > 100;
        """).fetchone()[0]
        green_long_duration = con.execute("""
            SELECT COUNT(*) FROM green_taxi
            WHERE (lpep_dropoff_datetime - lpep_pickup_datetime) > INTERVAL '24' HOUR;
        """).fetchone()[0]
        g_min_year, g_max_year = con.execute("""
            SELECT MIN(EXTRACT(year FROM lpep_pickup_datetime)), MAX(EXTRACT(year FROM lpep_pickup_datetime))
            FROM green_taxi;
        """).fetchone()
        logging.info(f"Green Taxi - Zero Passengers: {green_zero_passengers}, Zero Distance: {green_zero_distance}, Long Distance: {green_long_distance}, Long Duration: {green_long_duration}, Year Range: {g_min_year} - {g_max_year}")
        print(f"Green Taxi - Zero Passengers: {green_zero_passengers}, Zero Distance: {green_zero_distance}, Long Distance: {green_long_distance}, Long Duration: {green_long_duration}, Year Range: {g_min_year} - {g_max_year}")
        con.close()

    except Exception as e:
        logging.error(f"Error cleaning parquet files: {e}")
if __name__ == "__main__":
    clean_parquet()