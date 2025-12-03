# ⚡ AWS France Energy Weather Timeseries Pipeline

[![AWS](https://img.shields.io/badge/AWS-Cloud-orange?logo=amazonaws)](https://aws.amazon.com)
[![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)](https://python.org)
[![QuickSight](https://img.shields.io/badge/Amazon%20QuickSight-Visualization-orange?logo=amazonaws&logoColor=orange)](https://aws.amazon.com/quicksight/)
[![SQL](https://img.shields.io/badge/SQL-Query_Language-blue?logo=postgresql&logoColor=white)](https://en.wikipedia.org/wiki/SQL)
[![Parquet](https://img.shields.io/badge/Parquet-Columnar_Storage-50AB8C?logo=apacheparquet&logoColor=white)](https://parquet.apache.org)
[![Glue](https://img.shields.io/badge/AWS%20Glue-ETL_Service-blue?logo=amazonaws)](https://aws.amazon.com/glue/)
[![Lambda](https://img.shields.io/badge/AWS%20Lambda-Serverless-orange?logo=awslambda&logoColor=white)](https://aws.amazon.com/lambda/)
[![S3](https://img.shields.io/badge/Amazon%20S3-Data_Lake-569A31?logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)

A comprehensive cloud-native data engineering pipeline monitoring French electricity consumption correlated with weather patterns, holidays, and regional demographics. Built on AWS with full automation from data collection to business intelligence dashboards.

## Project Setup
```bash
aws-france-energy-weather-timeseries-pipeline/
├── 📁 etl/
│   ├── 📁 bronze_layer/                 # Fetch raw external data
│   │   ├── electricity_executor.py
│   │   ├── electricity_fetcher.py
│   │   ├── french_region_city_data.py
│   │   ├── holiday_fetcher.py
│   │   └── openmeteo_fetcher.py
│   │
│   ├── 📁 silver_layer/                 # Transform raw → structured (bronze → silver)
│   │   ├── data-engineering-bronze-to-silver-electricity_job.ipynb
│   │   ├── data-engineering-bronze-to-silver-holiday_job.ipynb
│   │   ├── data-engineering-bronze-to-silver-region_job.ipynb
│   │   └── data-engineering-bronze-to-silver-weather_job.ipynb
│   │
│   └── 📁 gold_layer/                   # Final analytical datasets & SQL views
│       ├── advanced_analytics_complete.py
│       ├── data-engineering-silver-to-gold_job.ipynb
│       ├── v_best_combined_forecast_overview.sql
│       ├── v_best_dayahead_forecast.sql
│       ├── v_best_intraday_forecast.sql
│       ├── v_complete_timeseries_normalized.sql
│       └── v_electricity_weather_daily.sql
│
├── 📁 infrastructure/                   # Infrastructure & automation
│   ├── data-engineering-project-ec2-advanced-analytics-instance-starter.py
│   └── data-engineering-project-ec2-instance-starter.py
│
├── .gitignore
├── README.md
└── requirements.txt
```
## 🏗️ Architecture Overview

### End-to-End Data Pipeline
```bash
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│   DATA SOURCES  │    │   DATA INGESTION │    │  DATA PROCESSING│    │   VISUALIZATION  │
│                 │    │                  │    │                 │    │                  │
│   RTE France    │────│   Python         │────│   AWS Glue      │────│   QuickSight     │
│   OpenMeteo     │    │   Scrapers on EC2│    │   ETL Pipelines │    │   Dashboards     │
│   Holidays API  │    │                  │    │   SQL Files     │    │                  │
│   Britannica    │    │   Lambda         │    │   in Gold Layer │    │   Analytics      │
│                 │    │   Orchestration  │    │                 │    │                  │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └──────────────────┘

DATA SOURCES
    │
    ├── RTE France (Electricity, 15-min intervals)
    ├── OpenMeteo (Weather, hourly)
    ├── Holidays API (Public holidays)
    └── Britannica (Region mapping)
    │
    ▼
DATA INGESTION (EC2 Instance 1 + Lambda)
    │
    ▼
S3 DATA LAKE
    ├── bronze/ (raw data)
    ├── silver/ (cleaned, transformed)
    └── gold/ (dimensions, facts, forecasts, SQL views, ML outputs)
    │
    ▼
AWS GLUE (ETL & Catalog)
    │
    ▼
ADVANCED ANALYTICS (EC2 Instance 2)
    ├── ML Model Training (Stacking: SARIMAX + XGBoost/MLP)
    ├── Cross-validation (Expanding/Sliding Window)
    └── Forecast Generation (Day-ahead/Intraday)
    │
    ▼
ANALYTICS STACK
    ├── Athena (Query engine on SQL views)
    ├── QuickSight (Dashboards)
    └── Gold Layer Updates (Advanced Analytics folder)
```
### Detailed Technical Architecture
```bash
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                AWS CLOUD PLATFORM                                      │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│  ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐                    │
│  │   SCHEDULING    │    │   COMPUTE LAYER  │    │   STORAGE LAYER │                    │
│  │                 │    │                  │    │                 │                    │
│  │   CloudWatch    │───▶│   Lambda        │───▶│   S3 Bucket     │                    │
│  │   Events        │    │   Functions      │    │   ┌───────────┐ │                    │
│  │                 │    │                  │    │   │  bronze/  │ │                    │
│  │   Cron:         │    │   EC2 Instance   │────│   │  silver/  │ │                    │
│  │    - Daily      │    │   (Scraping)     │    │   │  gold/    │ │                    │
│  │    - Weekly     │    │                  │    │   │  (incl.   │ │                    │
│  │    - Monthly    │    │   EC2 Instance   │────│   │   SQL)    │ │                    │
│  └─────────────────┘    │   (Advanced      │    │   └───────────┘ │                    │
│                         │    Analytics)    │    │                 │                    │
│                         └──────────────────┘    └─────────────────┘                    │
│                                  │                                                     │
│                                  ▼                                                     │
│                          ┌─────────────────┐                                           │
│                          │   DATA SOURCES  │                                           │
│                          │                 │                                           │
│                          │   RTE France    │──────────┐                                │
│                          │   OpenMeteo     │──────────┼┐                               │
│                          │   Holidays API  │──────────┼┼┐                              │
│                          │   Britannica    │──────────┼┼┼┐                             │
│                          └─────────────────┘          ││││                             │
│                                                       ││││                             │
│  ┌─────────────────┐    ┌──────────────────┐    ┌────▼▼▼▼┐      ┌─────────────────┐    │
│  │   PROCESSING    │    │    ANALYTICS     │    │   ETL   │     │BUSINESS INTELL. │    │
│  │                 │    │                  │    │         │     │                 │    │
│  │   AWS Glue      │──▶│   Data           │───▶│   SQL   │───▶│   QuickSight    │    │
│  │   Jobs          │    │   Warehouse      │    │   Files │     │   Dashboards    │    │
│  │                 │    │   (Athena)       │    │   Gold  │     │                 │    │
│  │   Transform     │    │                  │    │   Layer │     │   Insights      │    │
│  │   Bronze→Silver │    │   Aggregated     │    │         │     │                 │    │
│  │   Silver→Gold   │    │   Data Models    │    │         │     │                 │    │
│  │   + SQL Views   │    │                  │    │         │     │                 │    │
│  └─────────────────┘    └──────────────────┘    └─────────┘     └─────────────────┘    │
│                                                                                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```
## 📊 Data Sources & Frequency

| Data Source | Type | Frequency | Description                                       |
|-------------|------|-----------|---------------------------------------------------|
| **RTE France** | Electricity | Hourly | Real Time French grid consumption/production data |
| **OpenMeteo** | Weather | Hourly | Real Time Temperature history & forecasts         |
| **AbstractAPI** | Holidays | Monthly | French public holiday calendar                    |
| **Britannica** | Regions | Monthly | French administrative divisions                   |

## 🛠️ Technology Stack

### **Cloud Infrastructure (AWS)**
- **Compute**: EC2, Lambda Functions
- **Storage**: S3 (Data Lake)
- **Orchestration**: CloudWatch Events, IAM Roles
- **ETL**: AWS Glue, PySpark
- **Analytics**: Data Warehouse (Redshift/Athena)

### **Data Engineering**
- **Languages**: Python 3.9+
- **Libraries**: Pandas, Requests, BeautifulSoup, Boto3
- **Data Formats**: Parquet, CSV
- **Version Control**: Git, GitHub

### **Business Intelligence**
- **Visualization**: Tableau
- **Dashboarding**: Interactive BI Reports

## 🚀 Quick Start

### Prerequisites
- AWS Account with appropriate permissions
- Python 3.9+ environment
- Tableau Desktop/Server (for visualization)

### Installation
```bash
# Clone repository
git clone https://github.com/your-username/aws-france-energy-weather-timeseries-pipeline.git
cd aws-france-energy-weather-pipeline
```
### Complete EC2 Instance Setup
#### Prerequisites
- EC2 Instance running Amazon Linux 2023
- Elastic IP: 18.159.25.178 attached
- IAM Role with S3 access permissions

#### Step 1: Transfer Files to EC2 Instance
```bash
# update your Host
ssh-keygen -R "YOUR IPv4"

# Copy Python scripts to EC2 instance
scp -i your-key.pem *.py ec2-user@<YOUR PUBLIC IPv4>:/home/ec2-user/

# Copy requirements.txt to EC2 instance
scp -i your-key.pem requirements.txt ec2-user@<YOUR PUBLIC IPv4>:/home/ec2-user/

# Copy any additional configuration files
scp -i your-key.pem config.json ec2-user@<YOUR PUBLIC IPv4>:/home/ec2-user/
```
#### Step 2: SSH into EC2 Instance and Setup Environment
```bash
# Connect to your EC2 instance
ssh -i your-key.pem ec2-user@<YOUR PUBLIC IPv4>
```
#### Step 3: Python Environment Setup
```bash
# Update system packages
sudo yum update -y

# Install Python and development tools
sudo yum install -y python3 python3-pip python3-devel

# Create virtual environment in ec2-user home directory
python3 -m venv /home/ec2-user/venv

# Activate virtual environment
source /home/ec2-user/venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install required packages from requirements.txt
pip install -r /home/ec2-user/requirements.txt

# Verify installation
pip list
```

#### Step 4: Install Additional System Dependencies
```bash
# Install Chrome and ChromeDriver for Selenium
sudo yum install -y wget unzip

# Download and install Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
sudo yum install -y ./google-chrome-stable_current_x86_64.rpm

# Install ChromeDriver
wget https://storage.googleapis.com/chrome-for-testing-public/120.0.6099.109/linux64/chromedriver-linux64.zip
unzip chromedriver-linux64.zip
sudo mv chromedriver-linux64/chromedriver /usr/local/bin/
sudo chmod +x /usr/local/bin/chromedriver

# Verify Chrome and ChromeDriver installation
google-chrome --version
chromedriver --version
```

#### Step 5: Verify File Structure
```bash
# Check all files are in place
ls -la /home/ec2-user/

# Expected output:
# -rw-r--r-- 1 ec2-user ec2-user   electricity_executor.py
# -rw-r--r-- 1 ec2-user ec2-user   openmeteo_fetcher.py
# -rw-r--r-- 1 ec2-user ec2-user   holiday_fetcher.py
# -rw-r--r-- 1 ec2-user ec2-user   french_region_city_data.py
# -rw-r--r-- 1 ec2-user ec2-user   requirements.txt
# drwxr-xr-x 5 ec2-user ec2-user   venv/
```

#### Step 6: Test individual Scrapers
```bash
# Activate virtual environment
source /home/ec2-user/venv/bin/activate

# Test electricity scraper
python3 /home/ec2-user/electricity_executor.py

# Test weather scraper
python3 /home/ec2-user/openmeteo_fetcher.py

# Test holiday scraper
python3 /home/ec2-user/holiday_fetcher.py

# Test region-city scraper
python3 /home/ec2-user/french_region_city_data.py
```
#### Step 7: Set up Systemd Service for Automated Data Fetching
##### Overview
Configure a Systemd service with an intelligent scheduler that automatically runs data scrapers based on frequency requirements when the EC2 instance starts, and automatically shuts down after completion.

##### Create the Smart Scheduler
```bash
# Create the scheduler script
cat > /home/ec2-user/scheduler.py << 'EOF'
#!/usr/bin/env python3
"""
Smart scheduler that runs scrapers based on frequency requirements
- Hourly scrapers: openmeteo_fetcher.py, electricity_executor.py
- Monthly scrapers (1st of month): french_region_city_data.py, holiday_fetcher.py
"""
import subprocess
import sys
import os
from datetime import datetime

def run_scraper(script_name):
    """Run a Python scraper script"""
    try:
        print(f"Running {script_name}...")
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode == 0:
            print(f"✓ {script_name} completed successfully")
            return True
        else:
            print(f"✗ {script_name} failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Error running {script_name}: {e}")
        return False

def main():
    print("=== Starting Smart Scheduler ===")
    
    # Always run hourly scrapers
    print("=== Executing Hourly Scrapers ===")
    run_scraper("openmeteo_fetcher.py")
    run_scraper("electricity_executor.py")
    
    # Run monthly scrapers only on 1st day of month
    current_day = datetime.now().day
    if current_day == 1:
        print("=== First of Month - Executing Monthly Scrapers ===")
        run_scraper("french_region_city_data.py")
        run_scraper("holiday_fetcher.py")
    else:
        print(f"=== Skipping Monthly Scrapers (Day {current_day} of month) ===")
    
    print("=== All Scheduled Scrapers Completed ===")

if __name__ == "__main__":
    main()
EOF

# Make scheduler executable
chmod +x /home/ec2-user/scheduler.py
```

##### Create Systemd Service File
```bash
# Create Systemd service file
sudo tee /etc/systemd/system/scraper.service > /dev/null << 'EOF'
[Unit]
Description=Data Engineering Scrapers with Smart Scheduler
After=network.target

[Service]
Type=oneshot
User=ec2-user
WorkingDirectory=/home/ec2-user
ExecStartPre=/bin/sleep 30
ExecStart=/bin/bash -c 'source venv/bin/activate && python3 scheduler.py && sudo shutdown -h now'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
```
##### Enable and Start the Service
```bash
# Reload systemd daemon
sudo systemctl daemon-reload

# Enable service to start automatically on boot
sudo systemctl enable scraper.service

# Start the service immediately (optional - for testing)
sudo systemctl start scraper.service
```

##### Monitor Service Logs
```bash
# View real-time service logs
sudo journalctl -u scraper.service -f

# Check service status
sudo systemctl status scraper.service
```

##### How it Works
1. When the EC2 instance boots, the Systemd service automatically starts and executes the smart scheduler.
2. Intelligent Execution Order:
   - **Waits 30 seconds** for system stability
   - **Activates Python virtual environment**
   - **Runs smart scheduler** (`scheduler.py`) which determines which scrapers to execute:
       - **Always executes**: Weather data scraper (`openmeteo_fetcher.py`) and Electricity data scraper (`electricity_executor.py`)
       - **Conditionally executes** (1st day of month): Region/City data (`french_region_city_data.py`) and Holiday data (`holiday_fetcher.py`)
   - **Executes automatic shutdown** after completion
3. Self-Contained & Smart
   - No external triggers needed - the service handles the entire workflow.
   - Intelligent scheduling logic eliminates manual intervention.
   - Easy to modify frequencies by updating the scheduler script.

#### Step 8: Create requirements.txt (Local Development)
```bash
pandas>=1.5.0
requests>=2.28.0
beautifulsoup4>=4.11.0
boto3>=1.26.0
pyarrow>=10.0.0
openmeteo-requests>=1.0.0
requests-cache>=1.0.0
retry-requests>=1.0.0
selenium>=4.8.0
webdriver-manager>=3.8.0
python-dateutil>=2.8.0
urllib3<2.0  
```
