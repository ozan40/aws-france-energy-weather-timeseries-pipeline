# main.py
"""
Hauptscript für AWS Electricity Data Pipeline
- Ruft den Fetcher auf
- Lädt Daten in S3 Bucket
- Logging und Error Handling
"""

import logging
import sys
import os
from datetime import datetime
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

# Importiere unseren Fetcher
from electricity_fetcher import scrape_and_parse

# -----------------------------
# Configuration
# -----------------------------
S3_BUCKET = "data-engineering-project2-432801802552"
S3_PREFIX = "bronze_data/electricity_data"
AWS_REGION = "eu-central-1"


# -----------------------------
# Logging Setup
# -----------------------------
def setup_logging():
    """Setup für Logging auf EC2"""
    log_dir = "/home/ec2-user/logs"  # Geändert von /var/log zu home directory
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'{log_dir}/electricity_scraper.log')  # Geändert
        ]
    )


# -----------------------------
# S3 Functions
# -----------------------------
def upload_dataframe_to_s3(df, bucket, key):
    """
    Lädt einen DataFrame direkt als Parquet zu S3
    """
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)

        # DataFrame zu Parquet im Memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        # Upload zu S3
        s3_client.upload_fileobj(
            parquet_buffer,
            bucket,
            key
        )

        logging.info(f"Erfolgreich hochgeladen: s3://{bucket}/{key}")
        return True

    except Exception as e:
        logging.error(f"Fehler beim S3 Upload: {e}")
        return False


def generate_s3_key():
    """
    Generiert einen S3 Key mit Timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{S3_PREFIX}/france_electricity_{timestamp}.parquet"


# -----------------------------
# Main Function
# -----------------------------
def main():
    """
    Hauptfunktion - Wird vom Cron Job aufgerufen
    """
    setup_logging()
    logging.info("Starte Electricity Data Scraping Pipeline...")

    try:
        # Schritt 1: Daten scrapen
        logging.info("Starte Scraping Prozess...")
        electricity_data = scrape_and_parse()

        if electricity_data.empty:
            logging.warning("Keine Daten gescraped - Beende Prozess")
            return False

        logging.info(f"Erfolgreich {len(electricity_data)} Zeilen gescraped")

        # Schritt 2: Zu S3 hochladen
        s3_key = generate_s3_key()
        logging.info(f"Lade Daten hoch zu: {s3_key}")

        success = upload_dataframe_to_s3(
            electricity_data,
            S3_BUCKET,
            s3_key
        )

        if success:
            logging.info("Pipeline erfolgreich abgeschlossen!")
            return True
        else:
            logging.error("Pipeline fehlgeschlagen beim S3 Upload")
            return False

    except Exception as e:
        logging.error(f"Unerwarteter Fehler in der Pipeline: {e}")
        return False


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)