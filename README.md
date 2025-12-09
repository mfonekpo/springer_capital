# Springer Capital - Referral Data Analytics

A comprehensive Python data pipeline for loading, cleaning, profiling, and validating referral data from multiple CSV sources. This project processes referral information with business logic validation and generates detailed reports.

## 📋 Overview

This project implements an ETL (Extract, Transform, Load) pipeline that:
- **Loads** CSV files from the `DE Dataset - intern` directory
- **Cleans** data by standardizing formats, handling nulls, and fixing data types
- **Profiles** data to understand structure and quality
- **Validates** referrals against business logic rules
- **Generates** a comprehensive report with validation results

## 🏗️ Project Structure

```
springer_capital/
├── script/
│   ├── analytics.py              # Main entry point
│   ├── data_loader.py            # CSV loading utilities
│   ├── data_cleaner.py           # Data cleaning functions
│   ├── data_profiler.py          # Data profiling functions
│   ├── process.py                # Business logic & validation
│   ├── utility/
│   │   ├── pretty_print_df.py    # DataFrame display utilities
│   │   └── pretty_print_profiles.py # Profile display utilities
│   └── reports/
│       └── report.csv            # Generated output report
├── DE Dataset - intern/          # Input data directory
│   ├── lead_log.csv
│   ├── paid_transactions.csv
│   ├── referral_rewards.csv
│   ├── user_logs.csv
│   ├── user_referral_logs.csv
│   ├── user_referral_statuses.csv
│   └── user_referrals.csv
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Docker & Docker Compose (optional, for containerized execution)

### Local Installation

1. **Clone or navigate to the project directory**
   ```bash
   cd springer_capital
   ```

2. **Create a virtual environment** (recommended)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the pipeline**
   ```bash
   python script/analytics.py
   ```

   This will:
   - Load all CSV files from `DE Dataset - intern/`
   - Clean and profile the data
   - Print formatted tables and profiles to console
   - Generate `script/reports/report.csv`

### Docker Execution

1. **Build and run with Docker Compose**
   ```bash
   docker-compose up --build
   ```

2. **Access the output**
   - Console output will display data tables and profiles
   - Report will be generated in the `script/reports/` directory (mounted as a volume)

## 📊 Data Pipeline

### Stage 1: Data Loading (`data_loader.py`)
- Recursively loads all `.csv` files from a specified directory
- Returns a dictionary mapping filename → DataFrame
- Handles errors gracefully, continuing on file read failures

### Stage 2: Data Cleaning (`data_cleaner.py`)
Performs the following transformations:
- **Column standardization**: Lowercase, strip whitespace, replace spaces with underscores
- **Datetime conversion**: Converts date columns to timezone-aware timestamps
- **Type fixing**: Extracts numeric values from reward strings, converts booleans
- **Deduplication**: Removes duplicate rows
- **Null handling**: Preserves valid nulls while removing rows with critical missing keys

### Stage 3: Data Profiling (`data_profiler.py`)
Generates statistics for each column:
- Data type
- Row count
- Null count and percentage
- Distinct count and percentage
- Top value and frequency
- Min/Max (for numeric columns)

### Stage 4: Business Logic Validation (`process.py`)
Validates referrals based on complex business rules:

**Valid Success Condition:**
- Reward value > 0
- Status = "Berhasil" (Success)
- Transaction exists and is PAID
- Transaction type = NEW
- Transaction date > Referral date
- Same month (local time)
- Referrer membership not expired
- Referrer not deleted
- Referee reward granted

**Valid Pending/Failed Condition:**
- Status = "Menunggu" (Pending) or "Tidak Berhasil" (Failed)
- Reward value = 0 or null

**Invalid Conditions:**
- Reward given but status ≠ Success
- Reward given but no transaction
- Reward not given but transaction paid
- Success status without reward
- Transaction date before referral date

### Stage 5: Report Generation
Outputs a CSV report with:
- `referral_id`, `referrer_id`, `referee_id`
- `referral_at`, `referral_status`
- `transaction_id`, `transaction_status`, `transaction_type`
- `reward_value`, `referee_reward_granted`
- `referrer_membership_expired`, `referrer_is_deleted`
- `referral_source_category`
- **`is_business_logic_valid`** - Final validation result

## 🔧 Key Features

### Timezone Handling
- Converts all timestamps to local time using priority hierarchy:
  1. Transaction timezone
  2. Lead location timezone
  3. Referrer home club timezone
  4. Default: Asia/Jakarta

### Data Cleaning Features
- **Smart null handling** - Preserves expected nulls (transaction_id, reward_id)
- **Type coercion** - Safely handles mixed data types
- **Duplicate removal** - Maintains data integrity
- **Error resilience** - Continues processing on individual failures

### Visualization
Uses [Rich](https://rich.readthedocs.io/) library for beautiful console output:
- Formatted tables with truncation for long values
- Color-coded headers and data
- Row count indicators
- Profile statistics visualization

## 📦 Dependencies

Key packages:
- **pandas** (2.3.3) - Data manipulation and analysis
- **rich** (14.2.0) - Beautiful terminal output
- **python-dateutil** - Date utilities
- **pytz** - Timezone support

See `requirements.txt` for complete list.

## 📝 Configuration

### Environment Variables (`.env`)
```env
# Add as needed
PYTHONUNBUFFERED=1
```

### Input Data Format
All input files should be CSV with the following structure:
- **user_referrals.csv** - Main referral records
- **user_referral_logs.csv** - Referral event logs and reward grants
- **user_referral_statuses.csv** - Status definitions (Berhasil, Menunggu, Tidak Berhasil)
- **referral_rewards.csv** - Reward definitions with point values
- **paid_transactions.csv** - Transaction records
- **user_logs.csv** - User profile data
- **lead_log.csv** - Lead source data

## 🔍 Usage Examples

### Running the Full Pipeline
N/B: Before runing this command, ensure you download all dependencies in the ```requirements.txt``` file.
```bash
python script/analytics.py
```

### Importing as a Module
```python
from script.data_loader import data_loader
from script.data_cleaner import clean_all_tables
from script.process import process_and_validate_referrals

# Load data
data = data_loader("DE Dataset - intern")

# Clean data
cleaned = clean_all_tables(data)

# Validate
report = process_and_validate_referrals(cleaned)

# Use report
print(report[['referral_id', 'is_business_logic_valid']])
```

## 📈 Output

### Console Output
```
==================== Showing Tables of Cleaned Data =====================
[Formatted tables with first 30 rows of each dataset]

==================== Showing Profiles of Cleaned Data =====================
[Statistics for each column in each table]
```

### Generated Report
`/reports/report.csv` contains validation results for all referrals

## 🐳 Docker Deployment

The project is fully containerized and can be deployed using Docker:

```bash
# Build image
docker build -t springer-capital:latest .

# Run container
docker run -v $(pwd)/script/reports:/app/script/reports springer-capital:latest

# Or use Docker Compose for easier management
docker-compose up --build
```

## 📋 Requirements for Development

- Python 3.9+
- pandas >= 2.3.3
- rich >= 14.2.0
- All dependencies in `requirements.txt`

## 🤝 Contributing

When modifying the pipeline:
1. Update relevant stage modules (`data_cleaner.py`, `process.py`, etc.)
2. Test with sample data in `DE Dataset - intern/`
3. Verify output in `script/reports/report.csv`
4. Update this README with any changes to the pipeline

## ⚠️ Known Limitations

- Timezone handling assumes Asia/Jakarta as default (update in `process.py` line with `'Asia/Jakarta'`)
- Report generation requires write access to `script/reports/` directory
- Large datasets (>100MB) may require memory optimization

## 📞 Support

For issues or questions:
1. Check console error messages for data quality issues
2. Verify input CSV files exist in `DE Dataset - intern/`
3. Ensure all required columns are present in input files
4. Check `requirements.txt` versions match your environment

## 📄 License

Internal project for Springer Capital

---

**Last Updated:** 2025-01-09