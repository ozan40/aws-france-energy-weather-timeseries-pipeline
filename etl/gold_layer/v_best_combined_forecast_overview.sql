CREATE OR REPLACE VIEW "v_best_combined_forecast_overview" AS
WITH
  combined_best_forecasts AS (
   SELECT
     'intraday' forecast_type
   , forecast_timestamp
   , region_clean
   , forecast_horizon
   , model
   , avg_forecast_mw
   , null mae
   , null rmse
   , region_canon
   FROM
     v_best_intraday_forecast
UNION ALL    SELECT
     'dayahead' forecast_type
   , forecast_timestamp
   , region_clean
   , forecast_horizon
   , model
   , forecast_mw
   , mae
   , rmse
   , region_canon
   FROM
     v_best_dayahead_forecast
)
SELECT
  cf.forecast_type
, cf.forecast_timestamp
, cf.region_clean
, cf.region_canon
, cf.forecast_horizon
, cf.model
, cf.avg_forecast_mw
, cf.mae
, cf.rmse
, COALESCE(dh.holiday_name, 'No Holiday') holiday_name
, COALESCE(dh.is_national_holiday, 0) is_national_holiday
, EXTRACT(HOUR FROM cf.forecast_timestamp) forecast_hour
, EXTRACT(DOW FROM cf.forecast_timestamp) forecast_dow
, EXTRACT(MONTH FROM cf.forecast_timestamp) forecast_month
, (CASE WHEN (cf.rmse < 100) THEN 'Excellent' WHEN (cf.rmse < 500) THEN 'Good' WHEN (cf.rmse < 1000) THEN 'Fair' ELSE 'Poor' END) performance_rating
FROM
  (combined_best_forecasts cf
LEFT JOIN dim_holiday dh ON (DATE(cf.forecast_timestamp) = dh.date_paris))
ORDER BY cf.forecast_type ASC, cf.region_clean ASC, cf.forecast_timestamp ASC
