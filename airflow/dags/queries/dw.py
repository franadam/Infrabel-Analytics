import os
from dotenv import load_dotenv 

# loading variables from .env file
load_dotenv()

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ATHENA_DW_DATABASE = os.getenv("AWS_ATHENA_DW_DATABASE")

GEOGRAPHY_TABLE_NAME = 'geography'
PUNCTUALITY_TABLE_NAME = 'punctuality'

QUERY_CREATE_DW_DIM_DATE_TABLE: str = f"""
CREATE TABLE {AWS_ATHENA_DW_DATABASE}."dim_date"
WITH (
  format = 'PARQUET',
  external_location = 's3://{AWS_BUCKET_NAME}/{PUNCTUALITY_TABLE_NAME}/tables/parquet/date'
) AS
WITH boundaries AS (
  SELECT 
    min(cast(datdep AS date)) AS start_date, 
    max(cast(datdep AS date)) AS max_date
  FROM {AWS_ATHENA_DW_DATABASE}."stg_staging__punctuality_iceberg"
),
date_range AS (
  SELECT sequence(start_date, date_add('year', 5, max_date), interval '1' day) AS dt_array
  FROM boundaries
),
dates AS (
  SELECT dt AS date_value
  FROM date_range CROSS JOIN UNNEST(dt_array) AS t(dt)
)
SELECT
  cast(year(date_value) * 10000 + month(date_value) * 100 + day(date_value) AS integer) AS DateKey,
  cast(date_value as date) as Date,
  day(date_value) AS Day,
  CASE 
    WHEN day(date_value) IN (1, 21, 31) THEN 'st'
    WHEN day(date_value) IN (2, 22) THEN 'nd'
    WHEN day(date_value) IN (3, 23) THEN 'rd'
    ELSE 'th'
  END AS DaySuffix,
  day_of_week(date_value) AS Weekday,
  date_format(date_value, '%W') AS WeekDayName,
  upper(substr(date_format(date_value, '%W'), 1, 3)) AS WeekDayName_Short,
  substr(date_format(date_value, '%W'), 1, 1) AS WeekDayName_FirstLetter,
  day(date_value) AS DOWInMonth,
  day_of_year(date_value) AS DayOfYear,
  cast(floor(date_diff('day', date_trunc('month', date_value), date_value) / 7) + 1 AS integer) AS WeekOfMonth,
  week_of_year(date_value) AS WeekOfYear,
  month(date_value) AS Month,
  date_format(date_value, '%M') AS MonthName,
  upper(substr(date_format(date_value, '%M'), 1, 3)) AS MonthName_Short,
  substr(date_format(date_value, '%M'), 1, 1) AS MonthName_FirstLetter,
  cast((month(date_value) - 1) / 3 + 1 AS integer) AS Quarter,
  CASE cast((month(date_value) - 1) / 3 + 1 AS integer)
    WHEN 1 THEN 'First'
    WHEN 2 THEN 'Second'
    WHEN 3 THEN 'Third'
    WHEN 4 THEN 'Fourth'
  END AS QuarterName,
  year(date_value) AS Year,
  lpad(cast(month(date_value) AS varchar), 2, '0') || cast(year(date_value) AS varchar) AS MMYYYY,
  cast(year(date_value) AS varchar) || '/Q' || cast((month(date_value) - 1) / 3 + 1 AS varchar) AS QuarterYear,
  cast(year(date_value) AS varchar) || '/' || upper(substr(date_format(date_value, '%M'), 1, 3)) AS MonthYear,
  CASE 
    WHEN day_of_week(date_value) IN (1, 7) THEN true
    ELSE false
  END AS IsWeekend,
  false AS IsHoliday
FROM dates;
"""

QUERY_CREATE_DW_DIM_TIME_TABLE: str = f"""
CREATE TABLE "infrabel_dw_db"."dim_date"
WITH (
  format = 'PARQUET',
  external_location = 's3://{AWS_BUCKET_NAME}/{PUNCTUALITY_TABLE_NAME}/tables/parquet/dim/time'
) AS
WITH boundaries AS (
  SELECT 
    min(cast(datdep AS date)) AS start_date, 
    max(cast(datdep AS date)) AS max_date
  FROM {AWS_ATHENA_DW_DATABASE}."stg_staging__punctuality_iceberg"
),
date_range AS (
  SELECT sequence(start_date, date_add('year', 5, max_date), interval '1' day) AS dt_array
  FROM boundaries
),
dates AS (
  SELECT dt AS date_value
  FROM date_range CROSS JOIN UNNEST(dt_array) AS t(dt)
)
SELECT
  cast(year(date_value) * 10000 + month(date_value) * 100 + day(date_value) AS integer) AS DateKey,
  cast(date_value as date) as Date,
  day(date_value) AS Day,
  CASE 
    WHEN day(date_value) IN (1, 21, 31) THEN 'st'
    WHEN day(date_value) IN (2, 22) THEN 'nd'
    WHEN day(date_value) IN (3, 23) THEN 'rd'
    ELSE 'th'
  END AS DaySuffix,
  day_of_week(date_value) AS Weekday,
  date_format(date_value, '%W') AS WeekDayName,
  upper(substr(date_format(date_value, '%W'), 1, 3)) AS WeekDayName_Short,
  substr(date_format(date_value, '%W'), 1, 1) AS WeekDayName_FirstLetter,
  day(date_value) AS DOWInMonth,
  day_of_year(date_value) AS DayOfYear,
  cast(floor(date_diff('day', date_trunc('month', date_value), date_value) / 7) + 1 AS integer) AS WeekOfMonth,
  week_of_year(date_value) AS WeekOfYear,
  month(date_value) AS Month,
  date_format(date_value, '%M') AS MonthName,
  upper(substr(date_format(date_value, '%M'), 1, 3)) AS MonthName_Short,
  substr(date_format(date_value, '%M'), 1, 1) AS MonthName_FirstLetter,
  cast((month(date_value) - 1) / 3 + 1 AS integer) AS Quarter,
  CASE cast((month(date_value) - 1) / 3 + 1 AS integer)
    WHEN 1 THEN 'First'
    WHEN 2 THEN 'Second'
    WHEN 3 THEN 'Third'
    WHEN 4 THEN 'Fourth'
  END AS QuarterName,
  year(date_value) AS Year,
  lpad(cast(month(date_value) AS varchar), 2, '0') || cast(year(date_value) AS varchar) AS MMYYYY,
  cast(year(date_value) AS varchar) || '/Q' || cast((month(date_value) - 1) / 3 + 1 AS varchar) AS QuarterYear,
  cast(year(date_value) AS varchar) || '/' || upper(substr(date_format(date_value, '%M'), 1, 3)) AS MonthYear,
  CASE 
    WHEN day_of_week(date_value) IN (1, 7) THEN true
    ELSE false
  END AS IsWeekend,
  false AS IsHoliday
FROM dates;

"""
