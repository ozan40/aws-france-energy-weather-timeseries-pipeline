CREATE OR REPLACE VIEW "v_complete_timeseries_normalized" AS
SELECT
  FROM_UNIXTIME((CAST(ts_utc AS DOUBLE) / 1000000000)) ts_utc_timestamp
, consumption_mw
, region_clean
, temperature_c
, solar_production_mw
, wind_production_mw
, consumption_lag_1
, consumption_lag_2
, consumption_lag_3
, consumption_lag_6
, consumption_lag_12
, consumption_lag_24
, consumption_rolling_mean_3
, consumption_rolling_mean_6
, consumption_rolling_mean_12
, consumption_rolling_mean_24
, hour
, dow
, month
, hour_sin
, hour_cos
, dow_sin
, dow_cos
, data_type
, forecast_horizon
, model
, forecast_mw
FROM
  advanced_complete_timeseries
ORDER BY ts_utc_timestamp DESC
