# french_region_city_data.py
import requests
import pandas as pd
from bs4 import BeautifulSoup
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError
import time

# =========================
# Config / Outputs - EC2 & S3 Adapted
# =========================
URL = "https://www.britannica.com/topic/list-of-cities-and-towns-in-France-2039172"

# S3 Configuration
S3_BUCKET = "data-engineering-project-8433-3658-8863"
S3_BASE_PATH = "bronze_data/region_data"


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
    os.makedirs("/tmp/region_city_data", exist_ok=True)

    # Generate timestamp for unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = f"/tmp/region_city_data/{filename}_{timestamp}.parquet"
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
# Web Scraping Function
# =========================
def scrape_regions_cities(url: str = URL) -> pd.DataFrame:
    """
    Scrape French regions and their cities from Britannica website
    """
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36")
    }

    print("[INFO] Starting web scraping for French regions and cities...")

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        rows = []

        # section[data-level="1"] → h2.h1 > a (Region), daneben ul → li > div > a (Städte)
        for sec in soup.select('section[data-level="1"]'):
            reg_a = sec.select_one("h2.h1 > a")
            if not reg_a:
                continue
            region = reg_a.get_text(strip=True)

            city_links = sec.select('ul[class*="list-disc"] li div > a')
            if not city_links:
                city_links = sec.select("ul li div > a, ul li a")

            for a in city_links:
                city = a.get_text(strip=True)
                if city:
                    rows.append({"region": region, "city": city})

        df = pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)

        # Add metadata
        df["scraped_at"] = datetime.now()
        df["source_url"] = url

        print(f"[INFO] Successfully scraped {len(df)} city entries from {df['region'].nunique()} regions")
        return df

    except Exception as e:
        print(f"[ERROR] Web scraping failed: {e}")
        return pd.DataFrame()


def scrape_with_retry(url: str = URL, max_retries: int = 3) -> pd.DataFrame:
    """Scrape with retry logic in case of failures"""
    for attempt in range(max_retries):
        try:
            df = scrape_regions_cities(url)
            if not df.empty:
                return df
            else:
                print(f"[WARN] Attempt {attempt + 1} returned empty DataFrame")
        except Exception as e:
            print(f"[WARN] Attempt {attempt + 1} failed: {e}")

        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 10  # 10, 20, 30 seconds
            print(f"[INFO] Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print("[ERROR] All scraping attempts failed")
    return pd.DataFrame()


# =========================
# Data Enhancement Functions
# =========================
def enhance_region_data(df: pd.DataFrame) -> pd.DataFrame:
    """Add additional metadata and clean region names"""
    if df.empty:
        return df

    # Clean region names
    df["region_clean"] = df["region"].str.strip()

    # Add region counts
    region_counts = df["region"].value_counts().reset_index()
    region_counts.columns = ["region", "city_count"]
    df = df.merge(region_counts, on="region", how="left")

    # Add first letter for potential grouping
    df["city_first_letter"] = df["city"].str[0].str.upper()

    return df


# =========================
# Main - EC2/S3 Adapted
# =========================
def main():
    print("[INFO] Starting French Region-City Data Collection...")

    try:
        # 1) Scrape region-city data
        print("[INFO] Scraping region-city data from Britannica...")
        df = scrape_with_retry()

        if df.empty:
            print("[ERROR] No region-city data scraped.")
            return False

        # 2) Enhance data
        print("[INFO] Enhancing region-city data...")
        df_enhanced = enhance_region_data(df)

        print(f"[INFO] Data summary:")
        print(f"       - Regions: {df_enhanced['region'].nunique()}")
        print(f"       - Cities: {len(df_enhanced)}")
        print(f"       - Average cities per region: {df_enhanced['city_count'].mean():.1f}")

        # Show top regions by city count
        top_regions = df_enhanced.groupby('region')['city_count'].first().nlargest(5)
        print(f"       - Top regions: {list(top_regions.index)}")

        # 3) Save to S3 as Parquet
        print("[INFO] Saving data to S3...")
        s3_path = save_to_s3_parquet(df_enhanced, "french_region_city_mapping")

        if s3_path:
            print(f"[SUCCESS] Region-City data pipeline completed!")
            print(f"         S3 Location: s3://{S3_BUCKET}/{s3_path}")
            print(f"         Data Shape: {df_enhanced.shape}")
            print(f"         Regions: {df_enhanced['region'].nunique()}")
            print(f"         Cities: {len(df_enhanced)}")

            # Also save a sample as CSV for quick inspection
            sample_csv_path = f"/tmp/region_city_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_enhanced.head(100).to_csv(sample_csv_path, index=False)
            upload_to_s3(sample_csv_path, f"{S3_BASE_PATH}/region_city_sample.csv")

            return True
        else:
            print("[ERROR] Failed to upload to S3")
            return False

    except Exception as e:
        print(f"[ERROR] Region-City data collection failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)