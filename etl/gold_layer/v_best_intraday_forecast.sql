CREATE OR REPLACE VIEW "v_best_intraday_forecast" AS
WITH
  ranked_intraday_models AS (
   SELECT
     region_clean
   , forecast_horizon
   , model
   , COUNT(*) forecast_count
   , ROW_NUMBER() OVER (PARTITION BY region_clean, forecast_horizon ORDER BY COUNT(*) DESC, model ASC) rank_num
   FROM
     advanced_intraday_fact
   GROUP BY region_clean, forecast_horizon, model
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
, best_intraday_models AS (
   SELECT
     region_clean
   , forecast_horizon
   , model
   FROM
     ranked_intraday_models
   WHERE (rank_num = 1)
)
SELECT DISTINCT
  DATE_ADD('hour', aif.forecast_horizon, rla.last_actual_ts) forecast_timestamp
, aif.region_clean
, aif.data_type
, aif.forecast_horizon
, aif.model
, AVG(aif.mae) avg_mae
, AVG(aif.rmse) avg_rmse
, AVG(aif.forecast_mw) avg_forecast_mw
, dr.region_canon
, dr.cap_lat
, dr.cap_lon
, COALESCE(dh.holiday_name, 'No Holiday') holiday_name
, COALESCE(dh.is_national_holiday, 0) is_national_holiday
FROM
  ((((advanced_intraday_fact aif
INNER JOIN region_last_actual rla ON (aif.region_clean = rla.region_clean))
INNER JOIN dim_region dr ON (aif.region_clean = dr.region_clean))
INNER JOIN best_intraday_models bim ON ((aif.region_clean = bim.region_clean) AND (aif.forecast_horizon = bim.forecast_horizon) AND (aif.model = bim.model)))
LEFT JOIN dim_holiday dh ON (DATE(DATE_ADD('hour', aif.forecast_horizon, rla.last_actual_ts)) = dh.date_paris))
GROUP BY aif.forecast_horizon, rla.last_actual_ts, aif.region_clean, dr.region_canon, dr.cap_lat, dr.cap_lon, aif.data_type, aif.forecast_horizon, aif.model, dh.holiday_name, dh.is_national_holiday
ORDER BY aif.region_clean ASC, aif.forecast_horizon ASC
