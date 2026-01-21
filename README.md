# Data Pipeline & Analytics System

A Python-based data processing system that ingests raw transactional data, validates and transforms it, stores results in a database, and exposes insights via REST APIs.

## Features

- **Data Ingestion**: Read and validate CSV files with transaction data
- **Data Processing**: Aggregate sales by agent, retailer, and month; calculate commissions
- **Database Storage**: Persist data using SQLAlchemy with SQLite (easily switchable to PostgreSQL)
- **REST API**: FastAPI endpoints for querying reports and insights
- **Logging**: Structured logging with file and console output
- **Error Handling**: Graceful handling of bad files, invalid data, and DB errors

## Tech Stack

- Python 3
- pandas - Data manipulation
- FastAPI - REST API framework
- SQLAlchemy - ORM for database operations
- SQLite - Database (can switch to PostgreSQL)
- pytest - Testing framework
- uvicorn - ASGI server

## Project Structure

```
data-pipeline-python/
├── data/
│   ├── transactions.csv      # Input data
│   └── pipeline.db           # SQLite database
├── logs/
│   └── pipeline.log          # Application logs
├── src/
│   ├── api/
│   │   └── routes.py         # FastAPI endpoints
│   ├── ingestion/
│   │   └── reader.py         # CSV reading & validation
│   ├── processing/
│   │   └── aggregator.py     # Data aggregation & commission calc
│   ├── storage/
│   │   ├── models.py         # SQLAlchemy models
│   │   └── database.py       # Database operations
│   └── utils/
│       └── logger.py         # Logging configuration
├── tests/
│   ├── test_reader.py        # Ingestion tests
│   ├── test_aggregator.py    # Processing tests
│   └── test_api.py           # API tests
├── main.py                   # Entry point
├── requirements.txt          # Dependencies
└── README.md
```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd data-pipeline-python
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Run the Data Pipeline

Process transactions, calculate commissions, and store in database:

```bash
python3 main.py
```

### Run the API Server

Start the REST API server:

```bash
python3 main.py api
```

API will be available at:
- http://localhost:8000 - Root endpoint
- http://localhost:8000/docs - Interactive API documentation (Swagger UI)

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/agents` | GET | List all agents with commissions |
| `/agents/{agent_id}/commission` | GET | Get commission for specific agent |
| `/retailers` | GET | List all retailers with sales |
| `/retailers/{retailer_id}/sales` | GET | Get sales for specific retailer |
| `/reports/monthly` | GET | Monthly sales report |

### Run Tests

```bash
pytest tests/ -v
```

## Data Format

Input CSV file (`data/transactions.csv`) should have these columns:

| Column | Type | Description |
|--------|------|-------------|
| agent_id | string | Agent identifier |
| retailer_id | string | Retailer identifier |
| transaction_amount | float | Transaction amount |
| date | date | Transaction date (YYYY-MM-DD) |

Example:
```csv
agent_id,retailer_id,transaction_amount,date
A001,R001,1500.00,2024-01-15
A001,R002,2300.50,2024-01-16
A002,R001,1800.00,2024-01-15
```

## Business Rules

### Commission Calculation
- Sales < $5,000: 5% commission rate
- Sales >= $5,000: 8% commission rate

### Data Validation
Records are rejected if:
- Missing agent_id
- Missing retailer_id
- Missing or non-numeric transaction_amount
- Missing date

Rejected rows are logged to `logs/pipeline.log`.

## Database Schema

### Tables

**agents**
- id (PK)
- agent_id (unique)

**retailers**
- id (PK)
- retailer_id (unique)

**transactions**
- id (PK)
- agent_id (FK)
- retailer_id (FK)
- transaction_amount
- date

**commissions**
- id (PK)
- agent_id (FK)
- total_sales
- commission_rate
- commission_amount

## Switching to PostgreSQL

Update the database URL in `src/storage/database.py`:

```python
# SQLite (default)
db_url = "sqlite:///data/pipeline.db"

# PostgreSQL
db_url = "postgresql://user:password@localhost:5432/pipeline_db"
```

Also add `psycopg2` to requirements.txt:
```
psycopg2-binary
```

## License

MIT
