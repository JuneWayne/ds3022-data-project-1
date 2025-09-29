-- yellow taxi transformations
-- add CO2_PER_TRIP_KGS column
SELECT yellow_taxi.*,
    (
        yellow_taxi.trip_distance * (
            SELECT co2_grams_per_mile
            FROM vehicle_emissions
            WHERE vehicle_type = 'yellow_taxi'
        )
    ) / 1000 AS co2_per_trip_kgs,
    DATEDIFF(
        'second',
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    ) / 3600 AS trip_hours,
    CAST(strftime(tpep_pickup_datetime, '%u') AS INTEGER) AS trip_day_of_week,
    CAST(strftime(tpep_pickup_datetime, '%V') AS INTEGER) AS week_number,
    CAST(strftime(tpep_pickup_datetime, '%m') AS INTEGER) AS trip_month
FROM yellow_taxi