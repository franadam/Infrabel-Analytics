{{ config(materialized='table') }}

WITH
  hours AS (
    SELECT n AS hr
    FROM UNNEST(sequence(0,23)) AS t(n)
  ),
  minutes AS (
    SELECT n AS min
    FROM UNNEST(sequence(0,59)) AS t(n)
  ),
  seconds AS (
    SELECT n AS sec
    FROM UNNEST(sequence(0,59)) AS t(n)
  )
SELECT
  (hr * 3600 + min * 60 + sec) AS TimeKey,
  hr AS Hour24,
  lpad(cast(hr AS varchar), 2, '0') AS Hour24ShortString,
  concat(lpad(cast(hr AS varchar), 2, '0'), ':00') AS Hour24MinString,
  concat(lpad(cast(hr AS varchar), 2, '0'), ':00:00') AS Hour24FullString,
  mod(hr, 12) AS Hour12,
  lpad(cast(mod(hr, 12) AS varchar), 2, '0') AS Hour12ShortString,
  concat(lpad(cast(mod(hr, 12) AS varchar), 2, '0'), ':00') AS Hour12MinString,
  concat(lpad(cast(mod(hr, 12) AS varchar), 2, '0'), ':00:00') AS Hour12FullString,
  floor(hr / 12) AS AmPmCode,
  CASE WHEN hr < 12 THEN 'AM' ELSE 'PM' END AS AmPmString,
  min AS Minute,
  (hr * 100) + min AS MinuteCode,
  lpad(cast(min AS varchar), 2, '0') AS MinuteShortString,
  concat(lpad(cast(hr AS varchar), 2, '0'), ':', lpad(cast(min AS varchar), 2, '0'), ':00') AS MinuteFullString24,
  concat(lpad(cast(mod(hr, 12) AS varchar), 2, '0'), ':', lpad(cast(min AS varchar), 2, '0'), ':00') AS MinuteFullString12,
  floor(min / 30) AS HalfHour,
  (hr * 100) + (floor(min / 30) * 30) AS HalfHourCode,
  lpad(cast((floor(min / 30) * 30) AS varchar), 2, '0') AS HalfHourShortString,
  concat(lpad(cast(hr AS varchar), 2, '0'), ':', lpad(cast((floor(min / 30) * 30) AS varchar), 2, '0'), ':00') AS HalfHourFullString24,
  concat(lpad(cast(mod(hr, 12) AS varchar), 2, '0'), ':', lpad(cast((floor(min / 30) * 30) AS varchar), 2, '0'), ':00') AS HalfHourFullString12,
  sec AS Second,
  lpad(cast(sec AS varchar), 2, '0') AS SecondShortString,
  concat(
    lpad(cast(hr AS varchar), 2, '0'),
    ':',
    lpad(cast(min AS varchar), 2, '0'),
    ':',
    lpad(cast(sec AS varchar), 2, '0')
  ) AS FullTimeString24,
  concat(
    lpad(cast(mod(hr, 12) AS varchar), 2, '0'),
    ':',
    lpad(cast(min AS varchar), 2, '0'),
    ':',
    lpad(cast(sec AS varchar), 2, '0')
  ) AS FullTimeString12,
  date_format(from_unixtime(hr * 3600 + min * 60 + sec), '%H:%i:%S') AS FullTime
FROM hours
CROSS JOIN minutes
CROSS JOIN seconds;
