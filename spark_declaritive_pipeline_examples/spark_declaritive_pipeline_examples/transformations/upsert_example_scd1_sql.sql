-- This file shows how to handle SCD Type 1 CDC in SQL
-- SQL version of upsert_example_scd1.py

-- Step 1: Create temporary view for preprocessing
-- Note: SQL doesn't support utility functions like Python, so transformations are inline
CREATE TEMPORARY VIEW bronze_forecasts_preprocessed_sql AS
SELECT 
  * EXCEPT(audit_update_ts),
  -- Extract timezone offset
  REGEXP_EXTRACT(startTime, '([+-]\\d{2}:\\d{2})$', 1) AS timezoneOffset,
  -- Remove timezone from timestamps
  REGEXP_REPLACE(startTime, '[+-]\\d{2}:\\d{2}$', '') AS startTime_clean,
  REGEXP_REPLACE(endTime, '[+-]\\d{2}:\\d{2}$', '') AS endTime_clean,
  -- Convert to UTC
  CASE 
    WHEN REGEXP_EXTRACT(startTime, '([+-]\\d{2}:\\d{2})$', 1) != '' 
    THEN from_utc_timestamp(REGEXP_REPLACE(startTime, '[+-]\\d{2}:\\d{2}$', ''), REGEXP_EXTRACT(startTime, '([+-]\\d{2}:\\d{2})$', 1))
    ELSE REGEXP_REPLACE(startTime, '[+-]\\d{2}:\\d{2}$', '')
  END AS startTimeUTC,
  CASE 
    WHEN REGEXP_EXTRACT(endTime, '([+-]\\d{2}:\\d{2})$', 1) != '' 
    THEN from_utc_timestamp(REGEXP_REPLACE(endTime, '[+-]\\d{2}:\\d{2}$', ''), REGEXP_EXTRACT(endTime, '([+-]\\d{2}:\\d{2})$', 1))
    ELSE REGEXP_REPLACE(endTime, '[+-]\\d{2}:\\d{2}$', '')
  END AS endTimeUTC,
  -- Extract numeric windSpeed
  CAST(REGEXP_EXTRACT(windSpeed, '(\\d+)', 1) AS INT) AS windSpeed_clean,
  -- Extract nested values
  dewpoint.value AS dewpoint_value,
  probabilityOfPrecipitation.value AS probabilityOfPrecipitation_value,
  relativeHumidity.value AS relativeHumidity_value,
  -- Add audit columns (inline since SQL doesn't support utility functions)
  current_timestamp() AS audit_update_ts
FROM STREAM(leigh_robertson_demo.bronze_noaa.forecasts);

-- Step 2: Create target streaming table
CREATE OR REFRESH STREAMING TABLE forecasts_expanded_ldp_databricks_example_sql (
  CONSTRAINT valid_wind_speed EXPECT (windSpeed_clean >= 0)
)
COMMENT "SCD Type 1 managed silver forecasts"
TBLPROPERTIES ("quality" = "silver");

-- Step 3: Define Auto CDC flow
CREATE FLOW forecasts_cdc_flow AS AUTO CDC INTO forecasts_expanded_ldp_databricks_example_sql
FROM STREAM(bronze_forecasts_preprocessed_sql)
KEYS (post_code, startTime)
SEQUENCE BY audit_update_ts
STORED AS SCD TYPE 1;
