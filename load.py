import duckdb
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)
sleep_time = 1
def load_parquet_files():
    con = None
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        start_year, end_year = 2015, 2024

        # Loading yellow taxi table
        # Create table from first available month, then insert the rest
        created = False
        for year in range(start_year, end_year + 1):
            for m in range(1, 13):
                url = f"{base}/yellow_tripdata_{year}-{m:02d}.parquet"
                try:
                    # create a table if it doesn't exist, otherwise insert
                    if not created:
                        con.execute("""
                            CREATE OR REPLACE TABLE yellow_taxi AS
                            SELECT * FROM read_parquet(?, union_by_name=true)
                        """, [url])
                        logger.info(f"Initialized yellow_taxi from {url}")
                        print(f"Initialized yellow_taxi from {url}")
                        created = True
                    else:
                        con.execute("""
                            INSERT INTO yellow_taxi
                            SELECT * FROM read_parquet(?, union_by_name=true)
                        """, [url])
                        print(f"Inserted yellow {year}-{m:02d}")
                        logger.info(f"Inserted yellow {year}-{m:02d}")
                        
                        # sleep to avoid overwhelming the server or hitting rate limits
                        time.sleep(sleep_time)
                except Exception as e:
                    logger.info(f"Skip yellow {year}-{m:02d}: {e}")
                    print(f"Skip yellow {year}-{m:02d}: {e}")

        # loading green taxi table
        # Create table from first available month, then insert the rest
        created = False
        for year in range(start_year, end_year + 1):
            for m in range(1, 13):
                url = f"{base}/green_tripdata_{year}-{m:02d}.parquet"
                try:
                    # create a table if it doesn't exist, otherwise insert
                    if not created:
                        con.execute("""
                            CREATE OR REPLACE TABLE green_taxi AS
                            SELECT * FROM read_parquet(?, union_by_name=true)
                        """, [url])
                        logger.info(f"Initialized green_taxi from {url}")
                        print(f"Initialized green_taxi from {url}")
                        created = True
                    else:
                        con.execute("""
                            INSERT INTO green_taxi
                            SELECT * FROM read_parquet(?, union_by_name=true)
                        """, [url])
                        print(f"Inserted green {year}-{m:02d}")
                        logger.info(f"Inserted green {year}-{m:02d}")

                        # sleep to avoid overwhelming the server or hitting rate limits
                        time.sleep(sleep_time)
                # skip months that don't exist (just in case)
                except Exception as e:
                    logger.info(f"Skip green {year}-{m:02d}: {e}")
                    print(f"Skip green {year}-{m:02d}: {e}")

        # load vehicle_emissions data from CSV
        con.execute("""
            CREATE OR REPLACE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', header=true)
        """)
        logger.info("Created vehicle_emissions from CSV")

        # compute basic summary stats
        for table in ["yellow_taxi", "green_taxi"]:
            row_counts = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            logger.info(f"{table}: {row_counts:,} rows")
            print(f"{table}: {row_counts:,} rows")

            stats = con.execute(f"""
                SELECT
                    AVG(passenger_count) AS avg_passengers,
                    SUM(passenger_count) AS total_passengers,
                    AVG(trip_distance) AS avg_trip_distance,
                    SUM(trip_distance) AS total_trip_distance,
                    AVG(total_amount) AS avg_total_amount,
                    SUM(total_amount) AS total_revenue,
                    AVG(tip_amount) AS avg_tip_amount,
                    SUM(tip_amount) AS total_tips
                FROM {table};
            """).fetchone()

            print(
                f"average passenger count: {stats[0]:.2f} \n"
                f"total passengers: {stats[1]:.2f}\n"
                f"average trip distance: {stats[2]:.2f} \n"
                f"total trip distance: {stats[3]:.2f}\n"
                f"average total amount: {stats[4]:.2f} \n"
                f"total revenue: {stats[5]:.2f}\n"
                f"average tip amount: {stats[6]:.2f} \n"
                f"total tips received: {stats[7]:.2f}\n"
            )
            logger.info(f"average passenger count: {stats[0]:.2f} \n"
                f"total passengers: {stats[1]:.2f}\n"
                f"average trip distance: {stats[2]:.2f} \n"
                f"total trip distance: {stats[3]:.2f}\n"
                f"average total amount: {stats[4]:.2f} \n"
                f"total revenue: {stats[5]:.2f}\n"
                f"average tip amount: {stats[6]:.2f} \n"
                f"total tips received: {stats[7]:.2f}\n"
                )

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    finally:
        if con is not None:
            con.close()

if __name__ == "__main__":
    load_parquet_files()
