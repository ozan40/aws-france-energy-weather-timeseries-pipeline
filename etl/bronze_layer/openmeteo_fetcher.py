# openmeteo_data.py
import os, re, time, math
import pandas as pd
import requests
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Optional  # ✅ Import für Type Annotations

# --- Open-Meteo client bits
import openmeteo_requests
import requests_cache
from retry_requests import retry

# =========================
# Config / Outputs - EC2 & S3 Adapted
# =========================
# S3 Configuration
S3_BUCKET = "data-engineering-project-8433-3658-8863"
S3_BASE_PATH = "bronze_data/weather_data"

# How many cities to fetch (set to None for all — can be many API calls)
MAX_CITIES = 200
SLEEP_BETWEEN = 0.15  # politeness / avoid hammering

# OpenDataSoft (GeoNames cities >=1k pop)
BASE_URL = "https://data.opendatasoft.com"
DATASET = "geonames-all-cities-with-a-population-1000@public"

# Open-Meteo previous-runs endpoint + variables
OM_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
OM_HOURLY_VARS = [
    "temperature_2m",
    "temperature_2m_previous_day1",
    "temperature_2m_previous_day2",
    "temperature_2m_previous_day3",
    "temperature_2m_previous_day4",
    "temperature_2m_previous_day5",
]
OM_PAST_DAYS = 92


# =========================
# S3 Helper Functions
# =========================
def upload_to_s3(file_path: str, s3_key: str) -> bool:
    """Upload file to S3 bucket"""
    try:
        s3 = boto3.client('s3')
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"[SUCCESS] Uploaded to s3://{S3_BUCKET}/{s3_key}")
        return True
    except ClientError as e:
        print(f"[ERROR] S3 upload failed: {e}")
        return False


def save_to_s3_parquet(df: pd.DataFrame, filename: str) -> str:
    """Save DataFrame as Parquet and upload to S3"""
    # Create local temp directory
    os.makedirs("/tmp/weather_data", exist_ok=True)

    # Generate timestamp for unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = f"/tmp/weather_data/{filename}_{timestamp}.parquet"
    s3_key = f"{S3_BASE_PATH}/{filename}_{timestamp}.parquet"

    # Save locally
    df.to_parquet(local_path, index=False, engine='pyarrow')
    print(f"[INFO] Saved locally: {local_path}")

    # Upload to S3
    if upload_to_s3(local_path, s3_key):
        # Clean up local file
        try:
            os.remove(local_path)
        except:
            pass
        return s3_key
    return ""


# =========================
# Existing Helpers (unchanged)
# =========================
def ods_records_fetch_all_small(base_url=BASE_URL, dataset=DATASET, where=None, select=None,
                                order_by=None, page_size=100, sleep_sec=0.12, max_rows=10000):
    """
    Small helper to read OpenDataSoft V2 Explore API records.
    """
    url = f"{base_url}/api/explore/v2.1/catalog/datasets/{dataset}/records"
    frames, offset = [], 0
    while True:
        if offset >= max_rows:
            break
        params = {"limit": min(page_size, max_rows - offset), "offset": offset}
        if where:    params["where"] = where
        if select:   params["select"] = select
        if order_by: params["order_by"] = order_by
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("results") or payload.get("records") or []
        # Some datasets nest under "fields"
        if rows and isinstance(rows[0], dict) and "fields" in rows[0]:
            rows = [rec.get("fields", {}) for rec in rows]
        if not rows:
            break
        frames.append(pd.json_normalize(rows))
        got = len(rows)
        offset += got
        if got < params["limit"]:
            break
        if sleep_sec:
            time.sleep(sleep_sec)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _parse_coordinates(col):
    """
    Robustly extract lat/lon from different ODS formats:
    - dict with {'lat','lon'}
    - list/tuple [lon,lat]
    - 'POINT (lon lat)' text
    """
    lat, lon = None, None
    try:
        if isinstance(col, dict):
            lat = col.get("lat") or col.get("latitude")
            lon = col.get("lon") or col.get("longitude")
        elif isinstance(col, (list, tuple)) and len(col) >= 2:
            # ODS often uses [lon, lat]
            lon, lat = col[0], col[1]
        elif isinstance(col, str):
            m = re.search(r"POINT\s*\(([-\d\.]+)\s+([-\d\.]+)\)", col, flags=re.I)
            if m:
                lon, lat = float(m.group(1)), float(m.group(2))
    except Exception:
        pass
    return pd.to_numeric(pd.Series([lat, lon]), errors="coerce").tolist()


def normalize_fr_cities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure lat/lon columns exist and are numeric. Keep the essential columns.
    Deduplicate by geoname_id, prefer highest pop if duplicates sneak in.
    """
    df = df.copy()
    # Coordinate variants
    if "coordinates.lat" in df.columns and "coordinates.lon" in df.columns:
        df = df.rename(columns={"coordinates.lat": "lat", "coordinates.lon": "lon"})
    elif "coordinates" in df.columns and ("lat" not in df.columns or "lon" not in df.columns):
        lats, lons = [], []
        for x in df["coordinates"]:
            la, lo = _parse_coordinates(x)
            lats.append(la);
            lons.append(lo)
        df["lat"], df["lon"] = lats, lons

    # Minimal schema
    keep = ["geoname_id", "name", "country_code", "timezone", "population", "lat", "lon"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep]

    # Types & drops
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df[df["lat"].notna() & df["lon"].notna()]
    df = df[df["country_code"].astype(str).str.upper() == "FR"]

    # Deduplicate
    df = df.sort_values(["geoname_id", "population"], ascending=[True, False]).drop_duplicates("geoname_id")
    return df.reset_index(drop=True)


# =========================
# Open-Meteo client - FIXED Type Annotations
# =========================
_cache = requests_cache.CachedSession('.cache_openmeteo', expire_after=3600)
_retry = retry(_cache, retries=5, backoff_factor=0.25)
_openmeteo = openmeteo_requests.Client(session=_retry)


def fetch_openmeteo_hourly(lat: float, lon: float, past_days: int = OM_PAST_DAYS) -> Optional[pd.DataFrame]:  # ✅ FIXED
    """
    Fetch hourly Open-Meteo previous runs variables for a single lat/lon.
    Returns a DataFrame with 'date' + requested hourly variables (UTC).
    """
    params = {
        "latitude": float(lat),
        "longitude": float(lon),
        "hourly": OM_HOURLY_VARS,
        "past_days": int(past_days),
    }
    responses = _openmeteo.weather_api(OM_URL, params=params)
    if not responses:
        return None
    resp = responses[0]
    hourly = resp.Hourly()

    # MUST match order in OM_HOURLY_VARS
    arrs = [hourly.Variables(i).ValuesAsNumpy() for i in range(len(OM_HOURLY_VARS))]
    # Build time index
    t0 = pd.to_datetime(hourly.Time(), unit="s", utc=True)
    t1 = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True)
    step = pd.Timedelta(seconds=hourly.Interval())
    idx = pd.date_range(start=t0, end=t1, freq=step, inclusive="left")

    data = {"date": idx}
    for var_name, arr in zip(OM_HOURLY_VARS, arrs):
        data[var_name] = arr
    return pd.DataFrame(data)


def build_openmeteo_for_cities(df_cities: pd.DataFrame,
                               max_cities: Optional[int] = MAX_CITIES,  # ✅ FIXED
                               sleep_sec: float = SLEEP_BETWEEN) -> pd.DataFrame:
    """
    Loop cities → Open-Meteo → concat, with city metadata attached.
    """
    frames = []
    if max_cities is not None:
        df_iter = df_cities.head(max_cities)
    else:
        df_iter = df_cities

    for i, row in df_iter.iterrows():
        try:
            lat, lon = float(row["lat"]), float(row["lon"])
            h = fetch_openmeteo_hourly(lat, lon, past_days=OM_PAST_DAYS)
            if h is None or h.empty:
                print(f"[WARN] No OM data for {row['name']} ({lat},{lon})")
                continue
            # attach city metadata
            h.insert(0, "city_name", row["name"])
            h.insert(1, "geoname_id", row["geoname_id"])
            h["country_code"] = row["country_code"]
            h["population"] = row["population"]
            h["city_timezone"] = row["timezone"]
            h["lat"] = lat
            h["lon"] = lon
            frames.append(h)
            if sleep_sec:
                time.sleep(sleep_sec)
        except Exception as e:
            print(f"[WARN] {row.get('name', i)}: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =========================
# Main - EC2/S3 Adapted
# =========================
def main():
    print("[INFO] Starting Weather Data Collection...")

    try:
        # 1) Fetch French cities (>=1k pop), sorted by pop desc
        print("[INFO] Fetching French cities data...")
        df_fr = ods_records_fetch_all_small(
            where="country_code='FR' AND population >= 100000",
            select="geoname_id,name,country_code,timezone,population,coordinates",
            order_by="population DESC",
            max_rows=20000
        )
        if df_fr.empty:
            print("[ERROR] No French cities returned from OpenDataSoft.")
            return False

        df_fr = normalize_fr_cities(df_fr)
        print(f"[INFO] French cities ready: {df_fr.shape}")

        # 2) Open-Meteo for those cities
        print("[INFO] Fetching weather data from Open-Meteo...")
        weather = build_openmeteo_for_cities(df_fr, max_cities=MAX_CITIES, sleep_sec=SLEEP_BETWEEN)
        if weather.empty:
            print("[ERROR] No Open-Meteo data fetched.")
            return False

        # 3) Save to S3 as Parquet
        print("[INFO] Saving data to S3...")
        s3_path = save_to_s3_parquet(weather, "openmeteo_weather_french_cities")

        if s3_path:
            print(f"[SUCCESS] Weather data pipeline completed!")
            print(f"         S3 Location: s3://{S3_BUCKET}/{s3_path}")
            print(f"         Data Shape: {weather.shape}")
            print(f"         Date Range: {weather['date'].min()} to {weather['date'].max()}")
            return True
        else:
            print("[ERROR] Failed to upload to S3")
            return False

    except Exception as e:
        print(f"[ERROR] Weather data collection failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)