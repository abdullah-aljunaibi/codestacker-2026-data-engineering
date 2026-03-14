# Shipment Analytics Pipeline — CodeStacker 2026 Data Engineering Challenge

**Submitted by:** Abdullah Al Junaibi  
**Date:** March 10, 2026

---

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Docker Compose
- At least 4GB of available RAM

### Setup

```bash
# Clone the repository
git clone https://github.com/abdullah-aljunaibi/codestacker-2026-data-engineering.git
cd codestacker-2026-data-engineering

# Start all services
docker-compose up -d

# Wait ~2-3 minutes for Airflow to initialize, then verify:
curl http://localhost:8000/health    # Mock API
curl http://localhost:8080/health    # Airflow
```

### Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Mock API | http://localhost:8000 | — |
| PostgreSQL | localhost:5433 | airflow / airflow / airflow |

> **Note:** PostgreSQL is exposed on port **5433** (not 5432) to avoid conflicts with any existing PostgreSQL instance on the host.

### Run the Pipeline

**Option 1 — Via Airflow UI:**
1. Open http://localhost:8080
2. Enable the `shipment_analytics_pipeline` DAG
3. Click "Trigger DAG" (play button)

**Option 2 — Via command line:**
```bash
docker-compose exec airflow-webserver python -c "
import sys; sys.path.insert(0, '/opt/airflow/scripts')
from extract_shipments import extract_shipments_from_api
from extract_customer_tiers import extract_customer_tiers_from_csv
from transform_data import transform_shipment_data
from load_analytics import load_analytics_data

extract_shipments_from_api()
extract_customer_tiers_from_csv()
transform_shipment_data()
load_analytics_data()
"
```

### Check Results

```bash
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM analytics.shipping_spend_by_tier ORDER BY year_month, tier;"
```

Expected output: 11 rows showing shipping spend per customer tier per month (Jan–Mar 2024), with customer tier changes applied historically by effective date.

### Run Tests

```bash
# From host (requires Python 3.9+ and psycopg2):
pip install psycopg2-binary pytest
cd tests && python -m pytest test_pipeline.py -v
```

### Stop Services

```bash
docker-compose down       # Stop containers
docker-compose down -v    # Stop + remove all data
```

---

## Environment Variables

All pipeline scripts read credentials from environment variables (with defaults for Docker Compose):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `postgres` | Database hostname |
| `POSTGRES_DB` | `airflow` | Database name |
| `POSTGRES_USER` | `airflow` | Database user |
| `POSTGRES_PASSWORD` | `airflow` | Database password |
| `API_URL` | `http://api:8000/api/shipments` | Shipment API endpoint |
| `TIERS_CSV_PATH` | `/opt/airflow/data/customer_tiers.csv` | Customer tiers CSV path |

No credentials are hardcoded in any script.

---

## Project Structure

```
.
├── CHALLENGE_INSTRUCTIONS.md      # Original challenge brief
├── DISTRIBUTION_GUIDE.md          # Submission and evaluator notes
├── ENGINEERING_AUDIT.md           # Audit summary of identified issues and fixes
├── DESIGN_REFLECTION.md           # Design decisions and scaling notes
├── QUICK_START_EVALUATORS.md      # Fast-path setup for reviewers
├── README.md                      # Project overview and runbook
├── docker-compose.yml             # Local services and ports
├── Dockerfile
├── dags/
│   └── shipment_analytics_dag.py  # Airflow DAG orchestration
├── scripts/
│   ├── extract_shipments.py       # API extract with validation, retry, and dedup
│   ├── extract_customer_tiers.py  # CSV extract preserving customer tier history
│   ├── transform_data.py          # Effective-dated shipment-to-tier transformation
│   └── load_analytics.py          # Analytics load into monthly spend table
├── sql/
│   └── init.sql                   # Database schema initialization
├── data/
│   └── customer_tiers.csv         # Source: customer tier assignments
├── api/
│   ├── app.py                     # Mock shipment API (with deliberate flakiness)
│   └── Dockerfile
└── tests/
    ├── conftest.py                # Test configuration and DB helper
    ├── test_pipeline.py           # 23 pipeline tests across 4 categories
    ├── test_sample.py             # Minimal smoke test
    └── requirements.txt           # Test dependencies
```

---

## Data Flow

```
┌──────────────┐    ┌───────────────────┐    ┌────────────────┐    ┌──────────────────────┐
│  Mock API    │───▶│ extract_shipments │───▶│ transform_data │───▶│  load_analytics      │
│  (REST)      │    │ (validate, dedup) │    │ (LEFT JOIN)    │    │  (TRUNCATE+INSERT)   │
└──────────────┘    └───────────────────┘    └────────────────┘    └──────────────────────┘
                                                    ▲
┌──────────────┐    ┌───────────────────┐           │
│  CSV file    │───▶│ extract_tiers     │───────────┘
│              │    │ (tier history)    │
└──────────────┘    └───────────────────┘
```

**Pipeline output:** `analytics.shipping_spend_by_tier` — total shipping spend per customer tier per month.

---

## Issues Found & Fixed (Summary)

| # | Severity | Issue | Fix |
|---|----------|-------|-----|
| 1 | 🔴 Critical | SQL injection in extract_shipments | Parameterized queries |
| 2 | 🔴 Critical | YAML indentation error | Fixed `postgres:` indent |
| 3 | 🔴 Critical | API + Airflow port 8080 conflict | API remapped to 8000 |
| 4 | 🟠 High | Duplicate shipment SHP002 | Dedup by shipment_id |
| 5 | 🟠 High | No data validation | Reject negatives, nulls, cancelled |
| 6 | 🟠 High | Customer tier history lost | Preserve all tier rows and join by effective date |
| 7 | 🟠 High | INNER JOIN drops orphans | LEFT JOIN + COALESCE → 'Unknown' |
| 8 | 🟡 Medium | No API retry logic | 3 attempts, 5s delay |
| 9 | 🟡 Medium | Non-idempotent load | TRUNCATE before INSERT |
| 10 | 🟡 Medium | Non-atomic table swaps | Write _new, DROP old, RENAME |
| 11 | 🟡 Medium | Hardcoded credentials | Environment variables |
| 12 | 🔵 Low | Host postgres port conflict | Remapped to 5433 |
| 13 | 🔵 Low | API artificial latency | Handled via retry logic |

Full details in [`ENGINEERING_AUDIT.md`](./ENGINEERING_AUDIT.md).

---

## Test Coverage

`tests/test_pipeline.py` contains 23 pipeline tests across 4 categories:

| Category | Tests | What's Verified |
|----------|-------|-----------------|
| Extraction | 12 | Shipment validation, tier history preservation, schema contract, deterministic rejection of invalid tier-history inputs |
| Transformation | 4 | Row preservation, orphan→Unknown, effective-dated tiering, all tiers present |
| Analytics | 6 | Data exists, 11 rows, corrected monthly totals, no negatives, totals match, no dupes |
| Idempotency | 1 | TRUNCATE+INSERT produces identical results |

`tests/test_sample.py` remains as a separate lightweight smoke test.

---

## Technical Stack

- Apache Airflow 2.x (orchestration)
- PostgreSQL 13 (storage)
- Python 3.9 (pipeline scripts)
- Docker & Docker Compose (infrastructure)
- pytest (testing)
