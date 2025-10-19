# holiday_fetcher.py
import requests
import datetime
import calendar
import time
import os
import pandas as pd
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# =========================
# Config / Outputs - EC2 & S3 Adapted
# =========================
API_KEY = "bdef29c5025e41409cedea3b20018d03"  # Replace with your real key
BASE_URL = "https://holidays.abstractapi.com/v1/"

# S3 Configuration
S3_BUCKET = "data-engineering-project2-432801802552"
S3_BASE_PATH = "bronze_data/calendar_data"


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
    os.makedirs("/tmp/calendar_data", exist_ok=True)

    # Generate timestamp for unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = f"/tmp/calendar_data/{filename}_{timestamp}.parquet"
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
# Holiday Data Fetching
# =========================
def fetch_holidays_for_year(year: int = None) -> pd.DataFrame:
    """
    Fetch holidays for entire year and return as DataFrame
    """
    if year is None:
        year = datetime.now().year

    all_holidays = []

    print(f"[INFO] Fetching holidays for year {year}...")

    for month in range(1, 13):
        num_days = calendar.monthrange(year, month)[1]

        for day in range(1, num_days + 1):
            params = {
                "api_key": API_KEY,
                "country": "FR",
                "year": year,
                "month": month,
                "day": day
            }

            try:
                resp = requests.get(BASE_URL, params=params, timeout=30)

                # Handle rate limiting
                if resp.status_code == 429:
                    print(f"[WARN] Rate limit hit on {year}-{month:02d}-{day:02d}, waiting...")
                    time.sleep(5)  # Longer wait for rate limit
                    resp = requests.get(BASE_URL, params=params, timeout=30)

                if resp.status_code == 200:
                    daily_data = resp.json()
                    date_str = f"{year}-{month:02d}-{day:02d}"

                    if daily_data:
                        # Holiday found → mark is_holiday = True
                        for holiday in daily_data:
                            all_holidays.append({
                                "date": date_str,
                                "country": "France",
                                "is_holiday": True,
                                "holiday_name": holiday.get("name", ""),
                                "holiday_type": holiday.get("type", ""),
                                "location": holiday.get("location", "National")
                            })
                    else:
                        # No holiday for that day
                        all_holidays.append({
                            "date": date_str,
                            "country": "France",
                            "is_holiday": False,
                            "holiday_name": "",
                            "holiday_type": "",
                            "location": "National"
                        })
                else:
                    print(f"[ERROR] {resp.status_code} for {year}-{month:02d}-{day:02d}: {resp.text}")
                    # Still add the date with holiday=False for completeness
                    all_holidays.append({
                        "date": f"{year}-{month:02d}-{day:02d}",
                        "country": "France",
                        "is_holiday": False,
                        "holiday_name": "",
                        "holiday_type": "",
                        "location": "National"
                    })

            except Exception as e:
                print(f"[ERROR] Exception for {year}-{month:02d}-{day:02d}: {e}")
                # Add date with error flag
                all_holidays.append({
                    "date": f"{year}-{month:02d}-{day:02d}",
                    "country": "France",
                    "is_holiday": False,
                    "holiday_name": "",
                    "holiday_type": "error",
                    "location": "National"
                })

            # Be respectful to API - longer delay for free tier
            time.sleep(1.5)

        print(f"[INFO] Completed month {month}/12")

    return pd.DataFrame(all_holidays)


def fetch_current_year_holidays() -> pd.DataFrame:
    """Fetch holidays for current year"""
    current_year = datetime.now().year
    return fetch_holidays_for_year(current_year)


def fetch_multiple_years_holidays(years_back: int = 2) -> pd.DataFrame:
    """Fetch holidays for multiple years (current year + previous years)"""
    current_year = datetime.now().year
    all_years_data = []

    for year in range(current_year - years_back, current_year + 1):
        year_data = fetch_holidays_for_year(year)
        all_years_data.append(year_data)
        print(f"[INFO] Completed year {year}")

    return pd.concat(all_years_data, ignore_index=True)


# =========================
# Main - EC2/S3 Adapted
# =========================
def main():
    print("[INFO] Starting Holiday Data Collection...")

    try:
        # Option 1: Only current year (faster)
        print("[INFO] Fetching holiday data for current year...")
        holidays_df = fetch_current_year_holidays()

        # Option 2: Multiple years (uncomment if needed)
        # print("[INFO] Fetching holiday data for multiple years...")
        # holidays_df = fetch_multiple_years_holidays(years_back=2)

        if holidays_df.empty:
            print("[ERROR] No holiday data fetched.")
            return False

        # Add metadata columns
        holidays_df["data_retrieved_at"] = datetime.now()
        holidays_df["year"] = pd.to_datetime(holidays_df["date"]).dt.year
        holidays_df["month"] = pd.to_datetime(holidays_df["date"]).dt.month
        holidays_df["day"] = pd.to_datetime(holidays_df["date"]).dt.day

        print(f"[INFO] Holiday data ready: {holidays_df.shape}")
        print(f"[INFO] Holidays found: {holidays_df['is_holiday'].sum()} days")

        # 3) Save to S3 as Parquet
        print("[INFO] Saving data to S3...")
        s3_path = save_to_s3_parquet(holidays_df, "french_holidays")

        if s3_path:
            print(f"[SUCCESS] Holiday data pipeline completed!")
            print(f"         S3 Location: s3://{S3_BUCKET}/{s3_path}")
            print(f"         Data Shape: {holidays_df.shape}")
            print(f"         Date Range: {holidays_df['date'].min()} to {holidays_df['date'].max()}")
            print(f"         Total Holidays: {holidays_df['is_holiday'].sum()}")
            return True
        else:
            print("[ERROR] Failed to upload to S3")
            return False

    except Exception as e:
        print(f"[ERROR] Holiday data collection failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)