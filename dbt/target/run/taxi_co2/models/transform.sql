
  
  create view "emissions"."main"."transform__dbt_tmp" as (
    { { config(materialized = 'table') } } -- yellow taxi transformations
-- add CO2_PER_TRIP_KGS column
ALTER TABLE yellow_taxi
ADD COLUMN IF NOT EXISTS co2_per_trip_kgs DOUBLE;
UPDATE yellow_taxi y
SET co2_per_trip_kgs = (y.trip_distance * v.co2_grams_per_mile) / 1000.0
FROM vehicle_emissions v
WHERE v.vehicle_type = 'yellow_taxi';
-- ALTER TABLE yellow_taxi
-- ADD COLUMN IF NOT EXISTS AVG_MPH_PER_TRIP FLOAT;
-- ALTER TABLE yellow_taxi
-- ADD COLUMN IF NOT EXISTS TRIP_HOURS FLOAT;
-- ALTER TABLE yellow_taxi
-- ADD COLUMN IF NOT EXISTS TRIP_DAY_OF_WEEK INT;
-- ALTER TABLE yellow_taxi
-- ADD COLUMN IF NOT EXISTS WEEK_NUMBER INT;
-- ALTER TABLE yellow_taxi
-- ADD COLUMN IF NOT EXISTS MONTH_NUMBER INT;
  );
