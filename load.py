import duckdb
import os
import logging
import requests
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():
    con = None
    try:
        os.makedirs('data/taxi/yellow', exist_ok=True)
        os.makedirs('data/taxi/green',  exist_ok=True)

        base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        start_year, end_year = 2015, 2024

        # loop through all the years and months to download yellow and green taxi parquet files through requests
        for year in range(start_year, end_year + 1):
            for m in range(1, 13):
                url = f"{base}/yellow_tripdata_{year}-{m:02d}.parquet"
                out = f"data/taxi/yellow/yellow_{year}-{m:02d}.parquet"

                # if parquet already exist, skip the download (for resuming if download is interrupted)
                if os.path.exists(out) and os.path.getsize(out) > 0:
                    logger.info(f"skipped {out} (exists, {os.path.getsize(out)/1_048_576:.1f} MB)")
                else:
                    os.makedirs(os.path.dirname(out), exist_ok=True)
                    r = requests.get(url, stream=True, timeout=60)
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))
                    desc = f"yellow-{year}-{m:02d}"
                    
                    # print out a progress bar with tqdm
                    bar = tqdm(total=total, unit="B", unit_scale=True, desc=desc) if total > 0 else tqdm(unit="B", unit_scale=True, desc=desc)

                    # stream download the file in chunks of 1 MB
                    with open(out, "wb") as f:
                        for chunk in r.iter_content(1024 * 1024):  
                            # skip empty chunks
                            if not chunk: continue
                            f.write(chunk)
                            # update the progress bar
                            bar.update(len(chunk))
                    bar.close()
                    logger.info(f"done {out}")
        
        # the same loop logic and progress bar update for green taxi files
        for year in range(start_year, end_year + 1):
            for m in range(1, 13):
                g_url = f"{base}/green_tripdata_{year}-{m:02d}.parquet"   
                g_out = f"data/taxi/green/green_{year}-{m:02d}.parquet"   

                if os.path.exists(g_out) and os.path.getsize(g_out) > 0:
                    logger.info(f"skipped {g_out} (exists, {os.path.getsize(g_out)/1_048_576:.1f} MB)")
                else:
                    os.makedirs(os.path.dirname(g_out), exist_ok=True)
                    r = requests.get(g_url, stream=True, timeout=60)
                    if r.status_code == 404:
                        logger.info(f"[MISS] {g_url} (404)"); continue    
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))
                    desc = f"green-{year}-{m:02d}"                     
                    bar = tqdm(total=total, unit="B", unit_scale=True, desc=desc) if total > 0 else tqdm(unit="B", unit_scale=True, desc=desc)

                    with open(g_out, "wb") as f:
                        for chunk in r.iter_content(1024 * 1024):
                            if not chunk: continue
                            f.write(chunk)
                            bar.update(len(chunk))
                    bar.close()
                    logger.info(f"done {g_out}")

        # load parquet files into duckdb
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Create three tables, one for each dataset
        con.execute("""
            CREATE OR REPLACE TABLE yellow_taxi AS
            SELECT * FROM read_parquet('data/taxi/yellow/*.parquet', union_by_name=true);

            CREATE OR REPLACE TABLE green_taxi AS
            SELECT * FROM read_parquet('data/taxi/green/*.parquet', union_by_name=true);

            CREATE OR REPLACE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', header=true);
        """)

        # basic summary stats:
        for table in ["yellow_taxi", "green_taxi"]:
            # row counts
            row_counts = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            logger.info(f"{table}: {row_counts:,} rows")
            print(f"{table}: {row_counts:,} rows")

            # compute basic summary stats of column averages and totals
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
                FROM {table};""").fetchone()
            print_message = (f"average passenger count: {stats[0]:.2f} \ntotal passengers: {stats[1]:.2f}\n"
                             f"average trip distance: {stats[2]:.2f} \ntotal trip distance: {stats[3]:.2f}\n"
                             f"average total amount: {stats[4]:.2f} \ntotal revenue: {stats[5]:.2f}\n"
                             f"average tip amount: {stats[6]:.2f} \ntotal tips received: {stats[7]:.2f}\n")
            print(print_message)
            logger.info(print_message)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()