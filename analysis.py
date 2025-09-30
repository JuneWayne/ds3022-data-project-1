import duckdb
import logging
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def analyze_data():
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logging.info("Connected to emissions database")
        print("Connected to emissions database")
        con.execute("PRAGMA memory_limit='4GB'") 

        def calculate_month_totals(y_rows, g_rows):
            # rows are list of (month, total)
            months_y = [m for m, i in y_rows]
            months_g = [m for m, i in g_rows]
            months = sorted(set(months_y) | set(months_g))
            y_map = dict(y_rows)
            g_map = dict(g_rows)
            yellow_totals = [y_map.get(m, 0) for m in months]
            green_totals  = [g_map.get(m, 0) for m in months]
            return months, yellow_totals, green_totals

        # SECTION 1: results across ALL years

        # largest trip CO2 values across all years
        (y_max_co2,) = con.execute("""
            SELECT MAX(trip_co2_kgs) FROM yellow_transformed
        """).fetchone()
        print(f"[ALL] Largest YELLOW trip CO2: {y_max_co2:.3f} kg")
        logging.info(f"Largest YELLOW trip CO2: {y_max_co2:.3f} kg")

        (g_max_co2,) = con.execute("""
            SELECT MAX(trip_co2_kgs) FROM green_transformed
        """).fetchone()
        print(f"[ALL] Largest GREEN trip CO2:  {g_max_co2:.3f} kg")
        logging.info(f"Largest GREEN trip CO2:  {g_max_co2:.3f} kg")

        # heavy and light hour across all years
        y_heavy_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY hour_of_day
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW heavy HOUR: {y_heavy_hour[0]} (avg {y_heavy_hour[1]:.3f} kg)")
        logging.info(f"YELLOW heavy HOUR: {y_heavy_hour[0]} (avg {y_heavy_hour[1]:.3f} kg)")

        y_light_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY hour_of_day
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW light HOUR: {y_light_hour[0]} (avg {y_light_hour[1]:.3f} kg)")
        logging.info(f"[ALL] YELLOW light HOUR: {y_light_hour[0]} (avg {y_light_hour[1]:.3f} kg)")
        g_heavy_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY hour_of_day
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN heavy HOUR:  {g_heavy_hour[0]} (avg {g_heavy_hour[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN heavy HOUR:  {g_heavy_hour[0]} (avg {g_heavy_hour[1]:.3f} kg)")
        g_light_hour = con.execute("""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY hour_of_day
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN light HOUR:  {g_light_hour[0]} (avg {g_light_hour[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN light HOUR:  {g_light_hour[0]} (avg {g_light_hour[1]:.3f} kg)")

        # heavy and light day across all years
        y_heavy_day = con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY day_of_week
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW heavy DOW:  {y_heavy_day[0]} (avg {y_heavy_day[1]:.3f} kg)")
        logging.info(f"[ALL] YELLOW heavy DOW:  {y_heavy_day[0]} (avg {y_heavy_day[1]:.3f} kg)")

        y_light_day = con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY day_of_week
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW light DOW:  {y_light_day[0]} (avg {y_light_day[1]:.3f} kg)")
        logging.info(f"[ALL] YELLOW light DOW:  {y_light_day[0]} (avg {y_light_day[1]:.3f} kg)")

        g_heavy_day = con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY day_of_week
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN heavy DOW:   {g_heavy_day[0]} (avg {g_heavy_day[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN heavy DOW:   {g_heavy_day[0]} (avg {g_heavy_day[1]:.3f} kg)")

        g_light_day = con.execute("""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY day_of_week
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN light DOW:   {g_light_day[0]} (avg {g_light_day[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN light DOW:   {g_light_day[0]} (avg {g_light_day[1]:.3f} kg)")

        # heavy and light week across all years
        y_heavy_week = con.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY week_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW heavy WEEK: {y_heavy_week[0]} (avg {y_heavy_week[1]:.3f} kg)")
        logging.info(f"[ALL] YELLOW heavy WEEK: {y_heavy_week[0]} (avg {y_heavy_week[1]:.3f} kg)")

        y_light_week = con.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY week_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW light WEEK: {y_light_week[0]} (avg {y_light_week[1]:.3f} kg)")
        logging.info(f"[ALL] YELLOW light WEEK: {y_light_week[0]} (avg {y_light_week[1]:.3f} kg)")

        g_heavy_week = con.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY week_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN heavy WEEK:  {g_heavy_week[0]} (avg {g_heavy_week[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN heavy WEEK:  {g_heavy_week[0]} (avg {g_heavy_week[1]:.3f} kg)")

        g_light_week = con.execute("""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY week_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN light WEEK:  {g_light_week[0]} (avg {g_light_week[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN light WEEK:  {g_light_week[0]} (avg {g_light_week[1]:.3f} kg)")

        # heavy and light month across all years
        y_heavy_month = con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY month_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW heavy MONTH: {y_heavy_month[0]} (avg {y_heavy_month[1]:.3f} kg)")
        logging.info(f"[ALL] YELLOW heavy MONTH: {y_heavy_month[0]} (avg {y_heavy_month[1]:.3f} kg)")

        y_light_month = con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed
            GROUP BY month_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW light MONTH: {y_light_month[0]} (avg {y_light_month[1]:.3f} kg)")
        logging.info(f"[ALL] YELLOW light MONTH: {y_light_month[0]} (avg {y_light_month[1]:.3f} kg)")

        g_heavy_month = con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY month_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN heavy MONTH:  {g_heavy_month[0]} (avg {g_heavy_month[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN heavy MONTH:  {g_heavy_month[0]} (avg {g_heavy_month[1]:.3f} kg)")

        g_light_month = con.execute("""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed
            GROUP BY month_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN light MONTH:  {g_light_month[0]} (avg {g_light_month[1]:.3f} kg)")
        logging.info(f"[ALL] GREEN light MONTH:  {g_light_month[0]} (avg {g_light_month[1]:.3f} kg)")

        # calculate the monthly totals across all years plot
        y_monthly_all = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2_kg
            FROM yellow_transformed
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchall()
        g_monthly_all = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2_kg
            FROM green_transformed
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchall()

        months, yellow_totals, green_totals = calculate_month_totals(y_monthly_all, g_monthly_all)

        plt.figure(figsize=(10, 6))
        plt.plot(months, yellow_totals, marker='o', label='Yellow Taxi')
        plt.plot(months, green_totals,  marker='o', label='Green Taxi')
        plt.xticks(months)
        plt.xlabel('Month of Year')
        plt.ylabel('Total CO2 Emissions (kg)')
        plt.title('Monthly CO2 Emissions - All Years')
        plt.legend()
        plt.grid()
        plt.savefig('monthly_co2_emissions_all.png', dpi=150)
        print("Saved plot: monthly_co2_emissions_all.png")
        plt.close()

        # calculate the heaviest and lightest year across all years
        y_yearly = con.execute("""
            SELECT EXTRACT(year FROM tpep_pickup_datetime) AS yr,
                SUM(trip_co2_kgs) AS total_kg
            FROM yellow_transformed
            GROUP BY yr
            ORDER BY yr
        """).fetchall()
        years_y   = [r[0] for r in y_yearly]
        yellow_yr = [r[1] for r in y_yearly]

        g_yearly = con.execute("""
            SELECT EXTRACT(year FROM lpep_pickup_datetime) AS yr,
                SUM(trip_co2_kgs) AS total_kg
            FROM green_transformed
            GROUP BY yr
            ORDER BY yr
        """).fetchall()

        years_g  = [r[0] for r in g_yearly]
        green_yr = [r[1] for r in g_yearly]
        
        # highest / lowest year by TOTAL 
        y_high = con.execute("""
            SELECT EXTRACT(year FROM tpep_pickup_datetime) AS yr,
                SUM(trip_co2_kgs) AS total_kg
            FROM yellow_transformed
            GROUP BY yr
            ORDER BY total_kg DESC
            LIMIT 1
        """).fetchone()
        y_low = con.execute("""
            SELECT EXTRACT(year FROM tpep_pickup_datetime) AS yr,
                SUM(trip_co2_kgs) AS total_kg
            FROM yellow_transformed
            GROUP BY yr
            ORDER BY total_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] YELLOW highest total year: {y_high[0]} ({y_high[1]:.3f} kg)")
        print(f"[ALL] YELLOW lowest  total year: {y_low[0]} ({y_low[1]:.3f} kg)")

        g_high = con.execute("""
            SELECT EXTRACT(year FROM lpep_pickup_datetime) AS yr,
                SUM(trip_co2_kgs) AS total_kg
            FROM green_transformed
            GROUP BY yr
            ORDER BY total_kg DESC
            LIMIT 1
        """).fetchone()
        g_low = con.execute("""
            SELECT EXTRACT(year FROM lpep_pickup_datetime) AS yr,
                SUM(trip_co2_kgs) AS total_kg
            FROM green_transformed
            GROUP BY yr
            ORDER BY total_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[ALL] GREEN  highest total year: {g_high[0]} ({g_high[1]:.3f} kg)")
        print(f"[ALL] GREEN  lowest  total year: {g_low[0]} ({g_low[1]:.3f} kg)")
        plt.figure(figsize=(10, 6))
        plt.plot(years_y, yellow_yr, marker='o', label='Yellow Taxi')
        plt.plot(years_g, green_yr,  marker='o', label='Green Taxi')
        plt.xlabel('Year')
        plt.ylabel('Total CO2 Emissions (kg)')
        plt.title('Yearly CO2 Emissions - All Years')
        plt.legend()
        plt.grid()
        plt.tight_layout()
        plt.savefig('yearly_co2_emissions_all.png', dpi=150)
        plt.close()
        print("Saved plot: yearly_co2_emissions_all.png")
      

       

        


        # SECTION 2: Results for the year 2024 only

        # simple filter strings for 2024
        yt = "WHERE EXTRACT(year FROM tpep_pickup_datetime) = 2024"
        gt = "WHERE EXTRACT(year FROM lpep_pickup_datetime) = 2024"

        # largest trip CO2 in 2024
        (y_max_co2_2024,) = con.execute(f"""
            SELECT MAX(trip_co2_kgs) FROM yellow_transformed {yt}
        """).fetchone()
        print(f"[2024] Largest YELLOW trip CO2: {y_max_co2_2024:.3f} kg")
        logging.info(f"[2024] Largest YELLOW trip CO2: {y_max_co2_2024:.3f} kg")

        (g_max_co2_2024,) = con.execute(f"""
            SELECT MAX(trip_co2_kgs) FROM green_transformed {gt}
        """).fetchone()
        print(f"[2024] Largest GREEN trip CO2:  {g_max_co2_2024:.3f} kg")
        logging.info(f"[2024] Largest GREEN trip CO2:  {g_max_co2_2024:.3f} kg")

        # heavy and light hour in 2024
        y_heavy_hour_2024 = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY hour_of_day
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW heavy HOUR: {y_heavy_hour_2024[0]} (avg {y_heavy_hour_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW heavy HOUR: {y_heavy_hour_2024[0]} (avg {y_heavy_hour_2024[1]:.3f} kg)")

        y_light_hour_2024 = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY hour_of_day
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW light HOUR: {y_light_hour_2024[0]} (avg {y_light_hour_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW light HOUR: {y_light_hour_2024[0]} (avg {y_light_hour_2024[1]:.3f} kg)")

        g_heavy_hour_2024 = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY hour_of_day
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN heavy HOUR:  {g_heavy_hour_2024[0]} (avg {g_heavy_hour_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN heavy HOUR:  {g_heavy_hour_2024[0]} (avg {g_heavy_hour_2024[1]:.3f} kg)")

        g_light_hour_2024 = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY hour_of_day
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN light HOUR:  {g_light_hour_2024[0]} (avg {g_light_hour_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN light HOUR:  {g_light_hour_2024[0]} (avg {g_light_hour_2024[1]:.3f} kg)")

        # heavy and light day in 2024
        y_heavy_day_2024 = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY day_of_week
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW heavy DOW:  {y_heavy_day_2024[0]} (avg {y_heavy_day_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW heavy DOW:  {y_heavy_day_2024[0]} (avg {y_heavy_day_2024[1]:.3f} kg)")

        y_light_day_2024 = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY day_of_week
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW light DOW:  {y_light_day_2024[0]} (avg {y_light_day_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW light DOW:  {y_light_day_2024[0]} (avg {y_light_day_2024[1]:.3f} kg)")

        g_heavy_day_2024 = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY day_of_week
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN heavy DOW:   {g_heavy_day_2024[0]} (avg {g_heavy_day_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN heavy DOW:   {g_heavy_day_2024[0]} (avg {g_heavy_day_2024[1]:.3f} kg)")

        g_light_day_2024 = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY day_of_week
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN light DOW:   {g_light_day_2024[0]} (avg {g_light_day_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN light DOW:   {g_light_day_2024[0]} (avg {g_light_day_2024[1]:.3f} kg)")

        # heavy and light week in 2024
        y_heavy_week_2024 = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY week_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW heavy WEEK: {y_heavy_week_2024[0]} (avg {y_heavy_week_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW heavy WEEK: {y_heavy_week_2024[0]} (avg {y_heavy_week_2024[1]:.3f} kg)")

        y_light_week_2024 = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY week_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW light WEEK: {y_light_week_2024[0]} (avg {y_light_week_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW light WEEK: {y_light_week_2024[0]} (avg {y_light_week_2024[1]:.3f} kg)")

        g_heavy_week_2024 = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY week_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN heavy WEEK:  {g_heavy_week_2024[0]} (avg {g_heavy_week_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN heavy WEEK:  {g_heavy_week_2024[0]} (avg {g_heavy_week_2024[1]:.3f} kg)")

        g_light_week_2024 = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY week_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN light WEEK:  {g_light_week_2024[0]} (avg {g_light_week_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN light WEEK:  {g_light_week_2024[0]} (avg {g_light_week_2024[1]:.3f} kg)")

        # heavy and light month in 2024
        y_heavy_month_2024 = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY month_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW heavy MONTH: {y_heavy_month_2024[0]} (avg {y_heavy_month_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW heavy MONTH: {y_heavy_month_2024[0]} (avg {y_heavy_month_2024[1]:.3f} kg)")

        y_light_month_2024 = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM yellow_transformed {yt}
            GROUP BY month_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] YELLOW light MONTH: {y_light_month_2024[0]} (avg {y_light_month_2024[1]:.3f} kg)")
        logging.info(f"[2024] YELLOW light MONTH: {y_light_month_2024[0]} (avg {y_light_month_2024[1]:.3f} kg)")

        g_heavy_month_2024 = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY month_of_year
            ORDER BY avg_kg DESC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN heavy MONTH:  {g_heavy_month_2024[0]} (avg {g_heavy_month_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN heavy MONTH:  {g_heavy_month_2024[0]} (avg {g_heavy_month_2024[1]:.3f} kg)")

        g_light_month_2024 = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_kg
            FROM green_transformed {gt}
            GROUP BY month_of_year
            ORDER BY avg_kg ASC
            LIMIT 1
        """).fetchone()
        print(f"[2024] GREEN light MONTH:  {g_light_month_2024[0]} (avg {g_light_month_2024[1]:.3f} kg)")
        logging.info(f"[2024] GREEN light MONTH:  {g_light_month_2024[0]} (avg {g_light_month_2024[1]:.3f} kg)")

        # monthly totals for 2024 plot
        y_monthly_2024 = con.execute(f"""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2_kg
            FROM yellow_transformed {yt}
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchall()
        g_monthly_2024 = con.execute(f"""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2_kg
            FROM green_transformed {gt}
            GROUP BY month_of_year
            ORDER BY month_of_year
        """).fetchall()

        months24, yellow_totals24, green_totals24 = calculate_month_totals(y_monthly_2024, g_monthly_2024)

        plt.figure(figsize=(10, 6))
        plt.plot(months24, yellow_totals24, marker='o', label='Yellow Taxi')
        plt.plot(months24, green_totals24,  marker='o', label='Green Taxi')
        plt.xticks(months24)
        plt.xlabel('Month of Year')
        plt.ylabel('Total CO2 Emissions (kg)')
        plt.title('Monthly CO2 Emissions - 2024')
        plt.legend()
        plt.grid()
        plt.savefig('monthly_co2_emissions_2024.png', dpi=150)
        print("Saved plot: monthly_co2_emissions_2024.png")

        # close the connection
        con.close()

    except Exception as e:
        logging.error(f"Error during analysis: {e}")
        print(f"Error during analysis: {e}")

if __name__ == "__main__":
    analyze_data()
