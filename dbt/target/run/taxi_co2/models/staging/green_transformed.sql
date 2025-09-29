
  
    
    

    create  table
      "emissions"."main"."green_transformed__dbt_tmp"
  
    as (
      -- green taxi transformations
SELECT green_taxi.*,
    (
        -- add column to calculate CO2 emissions per trip in kilograms
        green_taxi.trip_distance * (
            SELECT co2_grams_per_mile
            FROM vehicle_emissions
            WHERE vehicle_type = 'green_taxi'
        )
    ) / 1000 AS trip_co2_kgs,
    -- add column to calculate average speed in miles per hour
    trip_distance / (
        date_diff(
            'second',
            lpep_pickup_datetime,
            lpep_dropoff_datetime
        ) / 3600.0
    ) AS avg_mph,
    -- add column to calculate hour of the day
    CAST(strftime(lpep_pickup_datetime, '%H') AS INTEGER) AS hour_of_day,
    -- add column to calculate the day of the week (1=Monday, 7=Sunday)
    CAST(strftime(lpep_pickup_datetime, '%u') AS INTEGER) AS day_of_week,
    -- add column to extract week number of the year
    CAST(strftime(lpep_pickup_datetime, '%V') AS INTEGER) AS week_of_year,
    -- add column to extract month number of the year
    CAST(strftime(lpep_pickup_datetime, '%m') AS INTEGER) AS month_of_year
FROM green_taxi
    );
  
  