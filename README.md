# ⚡ AWS France Energy Weather Timeseries Pipeline

[![AWS](https://img.shields.io/badge/AWS-Cloud-orange?logo=amazonaws)](https://aws.amazon.com)
[![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)](https://python.org)
[![Tableau](https://img.shields.io/badge/Tableau-Visualization-orange?logo=tableau)](https://tableau.com)

A comprehensive cloud-native data engineering pipeline monitoring French electricity consumption correlated with weather patterns, holidays, and regional demographics. Built on AWS with full automation from data collection to business intelligence dashboards.

## Project Setup
```bash
aws-france-energy-weather-pipeline/
├── 📁 scrapers/ # Data Collection
    ├── electricity_fetcher.py      # RTE Electricity Data Fetcher
│   ├── electricity_executor.py     # RTE Electricity execution
│   ├── openmeteo_fetcher.py           # Weather Data
│   ├── holiday_fetcher.py          # Holiday Calendar
│   └── french_region_city_data.py  # Regional Mapping
├── 📁 infrastructure/              # AWS Infrastructure
│   ├── lambda_ec2_orchestrator.py
│   ├── user_data_script.sh
│   └── cloudformation/             # IaC Templates
├── 📁 etl/                         # Data Processing
│   ├── glue_bronze_to_silver.py
│   ├── glue_silver_to_gold.py
│   └── data_validation/
├── 📁 tableau/                     # Visualization
│   ├── dashboards/
│   └── data_sources/
├── 📁 docs/                        # Documentation
│   ├── architecture.md
│   ├── setup_guide.md
│   └── api_references.md
└── 📁 tests/                       # Testing
    ├── unit_tests/
    └── integration_tests/
```
## 🏗️ Architecture Overview

### End-to-End Data Pipeline
```bash
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│   DATA SOURCES  │    │   DATA INGESTION │    │  DATA PROCESSING│    │   VISUALIZATION  │
│                 │    │                  │    │                 │    │                  │
│  ⚡ RTE France  │────│  🐍 Python     │────│  🪄 AWS Glue  │────│   📊 Tableau  │
│  🌤️ OpenMeteo   │    │  Scrapers on EC2 │    │  ETL Pipelines  │    │   Dashboards     │
│  🎉 Holidays API│    │                  │    │                 │    │                  │
│  🗺️ Britannica  │    │  ⚙️ Lambda      │    │  🗂️ S3 Data    │    │  📈 Analytics  │
│                 │    │  Orchestration   │    │  Lake           │    │                  │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └──────────────────┘
```
### Detailed Technical Architecture
```bash
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                AWS CLOUD PLATFORM                                       │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐                     │
│  │   SCHEDULING    │    │   COMPUTE LAYER  │    │   STORAGE LAYER │                     │
│  │                 │    │                  │    │                 │                     │
│  │  ⏰ CloudWatch │───▶│  λ Lambda        │───▶│  💼 S3 Bucket    │                   │
│  │    Events       │    │   Functions      │    │   ┌───────────┐ │                     │
│  │                 │    │                  │    │   │  bronze/  │ │                     │
│  │  📅 Cron:      │    │  🖥️ EC2 Instance │────│   │  silver/   │ │                    │
│  │   - Hourly      │    │   with User Data │    │   │  gold/    │ │                     │
│  │   - Monthly     │    │   Script         │    │   └───────────┘ │                     │
│  └─────────────────┘    └──────────────────┘    └─────────────────┘                     │
│                                  │                                                      │
│                                  ▼                                                      │
│                          ┌─────────────────┐                                            │
│                          │   DATA SOURCES  │                                            │
│                          │                 │                                            │
│                          │  ⚡ RTE France  │──────────┐                                 │
│                          │  🌤️ OpenMeteo  │──────────┐│                                 │
│                          │  🎉 Holidays API│───────┐ ││                                 │
│                          │  🗺️ Britannica  │─────┐ │ ││                                 │
│                          └─────────────────┘     │ │ │ │                                 │
│                                                  │ │ │ │                                 │
│  ┌─────────────────┐    ┌──────────────────┐    ┌▼─▼─▼─▼┐      ┌─────────────────┐       │
│  │   PROCESSING    │    │    ANALYTICS     │    │   ETL  │     │BUSINESS INTELL. │       │
│  │                 │    │                  │    │        │     │                 │       │
│  │  🪄 AWS Glue    │───▶│  🏢 Data        │───▶│  📊  │────▶│   📈 Tableau   │       │
│  │   Jobs          │    │   Warehouse      │    │  BI    │     │   Dashboards    │       │
│  │                 │    │                  │    │ Tools  │     │                 │       │
│  │  🔄 Transform   │    │  📊 Aggregated  │    │        │     │  💡 Insights    │       │
│  │   Bronze→Silver │    │   Data Models    │    │        │     │                 │        │
│  │   Silver→Gold   │    │                  │    │        │     │                 │        │
│  └─────────────────┘    └──────────────────┘    └────────┘     └─────────────────┘        │
│                                                                                           │
└───────────────────────────────────────────────────────────────────────────────────────────┘
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
Configure a Systemd service to automatically run data scrapers when the EC2 instance starts and automatically shut down after completion.
##### Create Systemd Service File
```bash
# Create Systemd service file
sudo tee /etc/systemd/system/scraper.service > /dev/null << 'EOF'
[Unit]
Description=Data Engineering Scrapers
After=network.target

[Service]
Type=oneshot
User=ec2-user
WorkingDirectory=/home/ec2-user
ExecStartPre=/bin/sleep 30
ExecStart=/bin/bash -c 'source venv/bin/activate && python3 openmeteo_fetcher.py && python3 electricity_executor.py && sudo shutdown -h now'
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
1. Automatic Startup: When the EC2 instance boots, the Systemd service automatically starts
2. Execution Order:
   - Waits 30 seconds for system stability
   - Activates Python virtual environment
   - Runs weather data scraper (openmeteo_fetcher.py)
   - Runs electricity data scraper (electricity_executor.py)
   - Executes automatic shutdown after completion
3. Self-Contained: No external triggers needed - the service handles the entire workflow

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
