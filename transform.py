import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="transform.log"
)
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------------------------
# DBT IS USED USED FOR TRANSFORMATION, THIS PYTHON SCRIPT IS CREATED JUST IN CASE IF IT IS NEEDED
# PLEASE REFER TO THE DBT DIRECTORY FOR TRANSFORMATION LOGIC
# -----------------------------------------------------------------------------------------------------

def transform_parquet():
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        log.info("Connected to emissions.duckdb database")
        print("Connected to emissions.duckdb database")
        con.execute("PRAGMA memory_limit='4GB'")

        # Transform yellow taxi data
        con.execute("""
            CREATE OR REPLACE TABLE yellow_transformed AS
            WITH base AS (
                SELECT
                    y.*,

                    -- calculate trip duration in seconds for each trip
                    DATEDIFF('second', y.tpep_pickup_datetime, y.tpep_dropoff_datetime) AS duration,

                    -- get the grams per mile for yellow taxis from vehicle_emissions table
                    (SELECT co2_grams_per_mile
                    FROM vehicle_emissions
                    WHERE vehicle_type = 'yellow_taxi') AS gpm
                FROM yellow_taxi y
            )
        SELECT
            *,
            -- add column to calculate CO2 emissions per trip in kilograms
            (trip_distance * gpm) / 1000.0 AS trip_co2_kgs,
          
            -- add column to calculate average speed in miles per hour, with guard against division by zero
            CASE WHEN duration > 0 THEN trip_distance / (duration / 3600.0) END AS avg_mph,
                    
            -- add column to calculate hour of the day
            CAST(strftime(tpep_pickup_datetime, '%H') AS INTEGER) AS hour_of_day,
                    
            -- add column to calculate day of the week (1=Monday, 7=Sunday)
            CAST(strftime(tpep_pickup_datetime, '%u') AS INTEGER) AS day_of_week, 

            -- add column to calculate week of the year
            CAST(strftime(tpep_pickup_datetime, '%V') AS INTEGER) AS week_of_year,  
                    
            -- add column to calculate month of the year
            CAST(strftime(tpep_pickup_datetime, '%m') AS INTEGER) AS month_of_year
        FROM base;
        """)
        print("Built yellow_transformed")
        log.info("Built yellow_transformed")
    
        # Transform green taxi data
        con.execute("""
            CREATE OR REPLACE TABLE green_transformed AS
            WITH base AS (
                SELECT
                    g.*,

                    -- calculate trip duration in seconds for each trip
                    DATEDIFF('second', g.lpep_pickup_datetime, g.lpep_dropoff_datetime) AS duration,

                    -- get the grams per mile for green taxis from vehicle_emissions table
                    (SELECT co2_grams_per_mile
                    FROM vehicle_emissions
                    WHERE vehicle_type = 'green_taxi') AS gpm
                FROM green_taxi g
            )
        SELECT
            *,
            -- add column to calculate CO2 emissions per trip in kilograms
            (trip_distance * gpm) / 1000.0 AS trip_co2_kgs,
                    
            -- add column to calculate average speed in miles per hour, with guard against division by zero
            CASE WHEN duration > 0 THEN trip_distance / (duration / 3600.0) END AS avg_mph,
            
            -- add column to calculate hour of the day
            CAST(strftime(lpep_pickup_datetime, '%H') AS INTEGER) AS hour_of_day,
                    
            -- add column to calculate day of the week (1=Monday, 7=Sunday)
            CAST(strftime(lpep_pickup_datetime, '%u') AS INTEGER) AS day_of_week,

            -- add column to calculate week of the year
            CAST(strftime(lpep_pickup_datetime, '%V') AS INTEGER) AS week_of_year,

            -- add column to calculate month of the year  
            CAST(strftime(lpep_pickup_datetime, '%m') AS INTEGER) AS month_of_year
        FROM base;
        """)
        print("Built green_transformed")
        log.info("Built green_transformed")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")
        log.error(f"An error occurred during transformation: {e}")
if __name__ == "__main__":
    transform_parquet()