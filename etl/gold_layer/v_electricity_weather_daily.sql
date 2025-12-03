CREATE OR REPLACE VIEW "v_electricity_weather_daily" AS
SELECT
  dt.ts_paris date_paris
, dr.region_clean
, dr.region_canon
, dr.cap_lat
, dr.cap_lon
, AVG(fe.consumption_mw) avg_daily_consumption
, SUM(fe.consumption_mw) total_daily_consumption
, AVG(fe.renewable_ratio) avg_renewable_ratio
, AVG(fw.temperature_c) avg_daily_temperature
, AVG(fw.humidity_percent) avg_humidity
, MAX(fw.precipitation_mm) max_daily_precipitation
, AVG(fw.wind_speed_kph) avg_wind_speed
, COALESCE(dh.holiday_name, 'No Holiday') holiday_name
, COALESCE(dh.holiday_type, 'No Holiday') holiday_type
, COALESCE(dh.is_national_holiday, 0) is_national_holiday
, COALESCE(dh.is_weekend_adjacent, 0) is_weekend_adjacent
, dt.day_name
, dt.month_name
, dt.season
, dt.is_weekend
, dt.is_workday
FROM
  ((((fact_electricity_hourly fe
INNER JOIN dim_time dt ON (fe.time_id = dt.time_id))
INNER JOIN dim_region dr ON (fe.region_id = dr.region_id))
LEFT JOIN dim_holiday dh ON (DATE(dt.ts_paris) = dh.date_paris))
LEFT JOIN fact_weather_hourly fw ON ((fe.time_id = fw.time_id) AND (fe.region_id = fw.region_id)))
GROUP BY dt.ts_paris, dr.region_clean, dr.region_canon, dr.cap_lat, dr.cap_lon, dh.holiday_name, dh.holiday_type, dh.is_national_holiday, dh.is_weekend_adjacent, dt.day_name, dt.month_name, dt.season, dt.is_weekend, dt.is_workday
ORDER BY date_paris DESC, dr.region_clean ASC
