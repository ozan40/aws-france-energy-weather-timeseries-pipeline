CREATE OR REPLACE VIEW "v_best_dayahead_forecast" AS
WITH
  ranked_dayahead_models AS (
   SELECT
     region_clean
   , forecast_horizon
   , model
   , forecast_mw
   , mae
   , rmse
   , ROW_NUMBER() OVER (PARTITION BY region_clean, forecast_horizon ORDER BY rmse ASC, mae ASC) rank_num
   FROM
     advanced_dayahead_fact
   WHERE (rmse IS NOT NULL)
)
, region_last_actual AS (
   SELECT
     region_clean
   , MAX(from_unixtime((CAST(ts_utc AS DOUBLE) / 1000000000))) last_actual_ts
   FROM
     advanced_complete_timeseries
   WHERE (data_type = 'actual')
   GROUP BY region_clean
)
, best_dayahead_models AS (
   SELECT
     region_clean
   , forecast_horizon
   , model
   , forecast_mw
   , mae
   , rmse
   FROM
     ranked_dayahead_models
   WHERE (rank_num = 1)
)
SELECT DISTINCT
  DATE_ADD('hour', bdm.forecast_horizon, rla.last_actual_ts) forecast_timestamp
, bdm.region_clean
, 'forecast' data_type
, bdm.forecast_horizon
, bdm.model
, bdm.forecast_mw
, bdm.mae
, bdm.rmse
, dr.region_canon
, dr.cap_lat
, dr.cap_lon
, EXTRACT(HOUR FROM DATE_ADD('hour', bdm.forecast_horizon, rla.last_actual_ts)) forecast_hour
, EXTRACT(DOW FROM DATE_ADD('hour', bdm.forecast_horizon, rla.last_actual_ts)) forecast_dow
, COALESCE(dh.holiday_name, 'No Holiday') holiday_name
, COALESCE(dh.is_national_holiday, 0) is_national_holiday
, COALESCE(dh.holiday_season, 'regular') holiday_season
FROM
  (((best_dayahead_models bdm
INNER JOIN region_last_actual rla ON (bdm.region_clean = rla.region_clean))
INNER JOIN dim_region dr ON (bdm.region_clean = dr.region_clean))
LEFT JOIN dim_holiday dh ON (DATE(DATE_ADD('hour', bdm.forecast_horizon, rla.last_actual_ts)) = dh.date_paris))
WHERE (bdm.forecast_horizon >= 24)
ORDER BY bdm.region_clean ASC, bdm.forecast_horizon ASC
