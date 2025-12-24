# Kasparro ETL System

A production-grade ETL (Extract, Transform, Load) pipeline built with FastAPI, PostgreSQL, and Docker. Designed for ingesting data from multiple sources (APIs, CSV, RSS) with robust error handling, resume-on-failure capability, and idempotent operations.

## 🌐 Live Demo

**Deployed on AWS EC2:** http://16.171.159.181:8000

Try the API:
- **Health Check:** http://16.171.159.181:8000/health
- **Data Endpoint:** http://16.171.159.181:8000/data
- **Statistics:** http://16.171.159.181:8000/stats
- **API Docs:** http://16.171.159.181:8000/docs

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Make (optional, for convenience commands)

### Setup & Run

**Option 1: Using Make (Recommended)**
```bash
make up
```

**Option 2: Using Docker Compose Directly**
```bash
# On Windows (PowerShell)
docker compose up -d --build

# On Linux/Mac
docker-compose up -d --build
```

### Initial Database Setup

After starting the containers for the first time, initialize the database:

```bash
# Install PostgreSQL extension
docker compose exec postgres psql -U etl_user -d etl_db -c "CREATE EXTENSION IF NOT EXISTS btree_gin;"

# Create database tables
docker compose exec api python scripts/init_db.py
```

## 🚀 Deployment

**This system is production-ready and designed for cloud deployment using Docker.**

### Deployment Overview

The Kasparro ETL system is containerized and can be deployed to any cloud platform (AWS EC2, GCP VM, Azure VM) or on-premises infrastructure.

**Quick Deployment Steps:**

1. **Provision VM** - Minimum 2 vCPU, 4GB RAM
2. **Install Docker + Docker Compose** - Use official installation scripts
3. **Configure Environment Variables** - Copy `.env.example` to `.env` and set your API keys
4. **Run `docker compose up -d`** - Start all services in detached mode
5. **Initialize Database** - Run setup scripts to create tables and extensions
6. **ETL Scheduling** - APScheduler runs automatically (every 30 minutes by default)

**Live Production Instance:** [http://16.171.159.181:8000](http://16.171.159.181:8000)

For detailed deployment instructions, see the [Cloud Deployment](#️-cloud-deployment) section below.

---

## 🏗️ Architecture

### System Overview

The Kasparro ETL system is a production-grade data pipeline that ingests data from multiple sources with robust error handling, automatic retry logic, and resume-on-failure capabilities.

**Key Design Principles:**
- **Idempotency** - Safe to re-run without duplicates
- **Resilience** - Retry logic, circuit breakers, and graceful degradation
- **Observability** - Comprehensive logging and metrics tracking
- **Maintainability** - Clear separation of concerns and extensive documentation

For detailed architecture documentation, see [ARCHITECTURE.md](file:///f:/kasparro-etl/ARCHITECTURE.md).

### System Components

```mermaid
graph TB
    subgraph "External Sources"
        API[REST APIs]
        CSV[CSV Files]
        RSS[RSS Feeds]
    end
    
    subgraph "ETL Pipeline"
        Scheduler[APScheduler<br/>30 min intervals]
        
        subgraph "Extract"
            APIExt[API Extractor<br/>+Retry +Circuit Breaker]
            CSVExt[CSV Extractor]
            RSSExt[RSS Extractor]
        end
        
        subgraph "Transform"
            Norm[Data Normalizer<br/>+Validation]
        end
        
        subgraph "Load"
            Loader[PostgreSQL Loader<br/>+Idempotent Upsert]
        end
        
        Runner[ETL Runner<br/>+Error Handling]
    end
    
    subgraph "Storage"
        DB[(PostgreSQL)]
        Raw[raw_data]
        Unified[unified_items]
        Runs[etl_runs]
        Checkpoints[etl_checkpoints]
    end
    
    subgraph "API Layer"
        FastAPI[FastAPI Server]
    end
    
    API --> APIExt
    CSV --> CSVExt
    RSS --> RSSExt
    
    Scheduler --> Runner
    Runner --> APIExt & CSVExt & RSSExt
    
    APIExt & CSVExt & RSSExt --> Raw
    Raw --> Norm
    Norm --> Loader
    Loader --> Unified
    
    Runner --> Runs & Checkpoints
    
    DB --> Raw & Unified & Runs & Checkpoints
    FastAPI --> DB
```

### Data Flow

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant R as Runner
    participant E as Extractor
    participant DB as Database
    participant N as Normalizer
    participant L as Loader
    
    S->>R: Trigger ETL (every 30 min)
    R->>DB: Get last checkpoint
    R->>E: run_incremental(checkpoint)
    
    alt Extraction Success
        E->>E: Fetch with retry logic
        E->>DB: Save to raw_data
        E->>R: Return records
        
        R->>DB: Fetch unprocessed raw_data
        
        loop For each record
            R->>N: normalize(record)
            alt Success
                N->>R: Normalized item
            else Failure
                N->>R: Log error, continue
            end
        end
        
        R->>L: load(items)
        L->>DB: UPSERT to unified_items
        R->>DB: Mark processed
        R->>DB: Update checkpoint
        R->>S: Success
    else Extraction Failure
        E->>R: Raise error
        R->>DB: Log failure
        R->>S: Failure (checkpoint not advanced)
    end
```

### Error Handling Strategy

The system implements a comprehensive error handling strategy with custom exceptions, retry logic, and circuit breakers:

**Error Classification:**
- **Retryable Errors** (with exponential backoff):
  - Network timeouts
  - HTTP 429 (Rate Limiting)
  - HTTP 5xx (Server Errors)
  - Database connection failures
  - Deadlocks

- **Non-Retryable Errors** (fail immediately):
  - HTTP 401/403 (Authentication)
  - HTTP 404 (Not Found)
  - Schema validation errors
  - Data format errors

**Retry Mechanism:**
- Exponential backoff: 1s, 2s, 4s, 8s...
- Configurable max retries (default: 3)
- Circuit breaker opens after 5 consecutive failures
- Circuit resets after 60 seconds

**Partial Failure Handling:**
- Individual record failures don't stop the pipeline
- Failed records are logged with detailed context
- Successfully processed records are committed
- ETL run marked as `PARTIAL` if some records fail

For complete error handling documentation, see [ARCHITECTURE.md](file:///f:/kasparro-etl/ARCHITECTURE.md#error-handling-architecture).

### Database Schema

```mermaid
erDiagram
    raw_data ||--o{ unified_items : "normalizes to"
    etl_runs ||--o{ raw_data : "tracks"
    etl_checkpoints ||--o{ etl_runs : "manages"
    
    raw_data {
        int id PK
        enum source_type
        string source_name
        jsonb raw_payload
        boolean processed
        timestamp ingested_at
    }
    
    unified_items {
        int id PK
        enum source_type
        string source_name
        string external_id UK
        string title
        jsonb metadata
    }
    
    etl_runs {
        uuid run_id PK
        enum status
        int records_extracted
        int records_loaded
        int records_failed
    }
    
    etl_checkpoints {
        int id PK
        string checkpoint_value
        timestamp last_success_at
    }
```

**Tables:**
- **`raw_data`** - Original source data with metadata (never deleted)
- **`unified_items`** - Normalized, deduplicated records (idempotent upserts)
- **`etl_runs`** - Execution history and metrics for monitoring
- **`etl_checkpoints`** - Resume points per source for failure recovery

### Key Features
- ✅ **Idempotent Operations** - Safe to re-run without duplicates
- ✅ **Resume on Failure** - Continues from last checkpoint
- ✅ **Transaction Safety** - Proper rollback handling
- ✅ **Multi-Source Support** - API, CSV, RSS feeds
- ✅ **Automated Scheduling** - Background ETL jobs via APScheduler
- ✅ **RESTful API** - Query and filter ingested data

## ⏰ Automated ETL Scheduling

**The ETL pipeline runs automatically via APScheduler - no manual cron setup required!**

### Scheduler Overview

The system uses **APScheduler** (Advanced Python Scheduler) integrated directly into the FastAPI application lifecycle. The scheduler starts automatically when the API container starts and runs ETL jobs at regular intervals.

**Key Details:**
- **Scheduler Type:** APScheduler (AsyncIOScheduler)
- **Default Interval:** Every **30 minutes**
- **Auto-Start:** Yes - starts with `docker compose up`
- **Manual Trigger:** Available via `scripts/run_etl.py`

### How It Works

1. **Automatic Startup**
   - When the API container starts, APScheduler initializes
   - ETL job is registered with a 30-minute interval trigger
   - First run occurs 30 minutes after startup

2. **Execution Flow**
   ```
   API Startup → APScheduler Init → Schedule ETL Job → Run Every 30 Min
   ```

3. **No Cron Required**
   - Unlike traditional ETL systems, no external cron jobs needed
   - Scheduler runs inside the application process
   - Survives container restarts (resumes from checkpoint)

### Configuration

#### Change Interval

Edit [`ingestion/scheduler.py`](file:///f:/kasparro-etl/ingestion/scheduler.py) (line 47):

```python
self.scheduler.add_job(
    self.run_etl_job,
    trigger=IntervalTrigger(minutes=30),  # Change interval here
    id="etl_job",
    replace_existing=True
)
```

**Examples:**
- Every 15 minutes: `minutes=15`
- Every hour: `minutes=60`
- Every 6 hours: `hours=6`
- Daily at midnight: Use `CronTrigger(hour=0, minute=0)` instead

#### Disable Automatic Scheduling

**Option 1: Comment out scheduler startup** (Recommended)

Edit [`api/main.py`](file:///f:/kasparro-etl/api/main.py):

```python
@app.on_event("startup")
async def startup_event():
    # etl_scheduler.start()  # Comment this line to disable
    logger.info("API started (scheduler disabled)")
```

**Option 2: Environment variable control**

Add to `.env`:
```env
ENABLE_SCHEDULER=false
```

Then modify `api/main.py`:
```python
if os.getenv("ENABLE_SCHEDULER", "true").lower() == "true":
    etl_scheduler.start()
```

#### Enable Manual-Only Mode

If you disable the scheduler, run ETL manually:

```bash
# Run ETL on-demand
docker compose exec api python scripts/run_etl.py

# Or set up external cron (on host machine)
0 * * * * docker compose -f /path/to/docker-compose.yml exec -T api python scripts/run_etl.py
```

### Verify Scheduler is Running

```bash
# Check API logs for scheduler messages
docker compose logs api | grep -i "scheduler"

# Expected output:
# "ETL Scheduler started"
# "Scheduler: Starting ETL job"
```

### Alternative: External Cron (Optional)

If you prefer traditional cron over APScheduler:

1. Disable APScheduler (see above)
2. Add cron job on host machine:

```bash
# Edit crontab
crontab -e

# Add entry (runs every 30 minutes)
*/30 * * * * cd /path/to/kasparro-etl && docker compose exec -T api python scripts/run_etl.py >> /var/log/etl-cron.log 2>&1
```

**Note:** APScheduler is recommended for production as it's container-native and doesn't require host-level cron access.

## 📋 Available Commands

### Using Make

| Command | Description |
|---------|-------------|
| `make up` | Start all services (Postgres + API) |
| `make down` | Stop all services |
| `make restart` | Restart all services |
| `make logs` | View logs from all services |
| `make logs-api` | View API logs only |
| `make logs-db` | View database logs only |
| `make shell` | Open shell in API container |
| `make db-shell` | Open PostgreSQL shell |
| `make test` | Run tests |
| `make clean` | Remove all containers and volumes |

### Using Docker Compose Directly

```bash
# Start services
docker compose up -d

# Stop services
docker compose down

# View logs
docker compose logs -f api

# Restart API
docker compose restart api

# Execute commands in container
docker compose exec api python scripts/run_etl.py
```

## 🔧 Running ETL Jobs

### Manual ETL Execution

```bash
docker compose exec api python scripts/run_etl.py
```

### Configure Data Sources

Edit `scripts/run_etl.py` to add your data sources:

```python
sources = [
    # API Source Example
    APIExtractor(
        db_session=session,
        source_name="coingecko_markets",
        api_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1"
    ),
    
    # CSV Source Example
    CSVExtractor(
        db_session=session,
        source_name="my_csv_data",
        file_path="/app/data/myfile.csv"
    ),
]
```

### Automated Scheduling

The ETL scheduler runs automatically when the API starts. Configure schedule in `ingestion/scheduler.py`.

## 🌐 API Endpoints

Once running, access the API at `http://localhost:8000`

### Health Check
```bash
# PowerShell
Invoke-RestMethod http://localhost:8000/health

# Linux/Mac
curl http://localhost:8000/health
```

### Get Data
```bash
# PowerShell
Invoke-RestMethod http://localhost:8000/data

# With filters
Invoke-RestMethod "http://localhost:8000/data?page=1&page_size=10&source_type=api"
```

### Get Statistics
```bash
Invoke-RestMethod http://localhost:8000/stats
```

### Interactive API Documentation
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 📁 Project Structure

```
kasparro-etl/
├── api/                    # FastAPI application
│   ├── routes/            # API endpoints
│   ├── middleware.py      # Request/response middleware
│   └── main.py           # Application entry point
├── ingestion/             # ETL pipeline
│   ├── extractors/       # Data source extractors
│   ├── transformers/     # Data normalization
│   ├── loaders/          # Database loaders
│   ├── runner.py         # ETL orchestration
│   └── scheduler.py      # Automated scheduling
├── models/                # SQLAlchemy ORM models
├── schemas/               # Pydantic validation schemas
├── core/                  # Configuration
├── scripts/               # Utility scripts
│   ├── init_db.py        # Database initialization
│   └── run_etl.py        # Manual ETL execution
├── tests/                 # Test suite
├── alembic/              # Database migrations
├── docker-compose.yml    # Docker orchestration
├── Dockerfile            # Container definition
└── requirements.txt      # Python dependencies
```

## 🔍 Troubleshooting

### Services Won't Start

```bash
# Check logs
docker compose logs

# Rebuild from scratch
docker compose down -v
docker compose build --no-cache
docker compose up -d
```

### Database Connection Issues

```bash
# Verify database is healthy
docker compose ps

# Check database logs
docker compose logs postgres

# Restart database
docker compose restart postgres
```

### ETL Failures

```bash
# Check API logs for errors
docker compose logs api --tail=100

# Verify database tables exist
docker compose exec postgres psql -U etl_user -d etl_db -c "\dt"

# Re-initialize database if needed
docker compose exec api python scripts/init_db.py
```

### "Make command not found" (Windows)

Use Docker Compose commands directly:
```powershell
docker compose up -d --build
docker compose exec api python scripts/run_etl.py
```

## 🧪 Testing

> **Note:** The test suite currently requires configuration updates to run inside Docker containers. The tests are configured to connect to `localhost:5432` but need to use `postgres:5432` when running in containers.

**Run tests locally** (if you have Python installed):
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html
```

**Run specific test file:**
```bash
pytest tests/unit/test_extractors.py -v
```

**Current Status:**
- ✅ Unit tests for extractors, transformers, loaders work
- ⚠️ Integration tests need database connection fix
- 📝 6 tests passing, 14 errors due to database connection configuration

## 🔐 Environment Variables

Copy `.env.example` to `.env` and configure:

```env
# Database
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_password
POSTGRES_DB=etl_db
DATABASE_URL=postgresql+asyncpg://etl_user:etl_password@postgres:5432/etl_db

# API
API_KEY=your_api_key_here
LOG_LEVEL=INFO

# ETL Configuration
BATCH_SIZE=1000
MAX_RETRIES=3
```

## 🔐 Secrets Management

**Security Best Practices:**

- ✅ **No secrets committed to repository** - All sensitive data excluded via `.gitignore`
- ✅ **`.env.example` contains placeholders only** - Safe template for developers
- ✅ **Real secrets injected via environment variables** - Configured at runtime, never in code
- ✅ **`.env` file gitignored** - Local secrets never tracked in version control

### How It Works

1. **Template File:** [`.env.example`](file:///f:/kasparro-etl/.env.example) contains placeholder values
   ```env
   API_KEY=YOUR_API_KEY_HERE  # ← Placeholder, not a real key
   ```

2. **Local Development:** Developers copy and populate with real values
   ```bash
   cp .env.example .env
   # Edit .env with actual API keys (never commit this file)
   ```

3. **Production Deployment:** Environment variables injected by cloud platform
   - AWS: EC2 user data, Secrets Manager, or Parameter Store
   - GCP: Secret Manager or instance metadata
   - Docker: `docker-compose.yml` reads from `.env` file (not committed)

4. **Runtime Injection:** Application reads secrets from environment
   ```python
   # core/config.py
   API_KEY = os.getenv("API_KEY")  # Loaded at runtime
   ```

### Verification

```bash
# Verify .env is gitignored
git check-ignore .env
# Output: .env (confirmed ignored)

# Verify no secrets in repository
git grep -i "api_key" -- ':!.env.example'
# Should only show placeholder references

# Run secret detection script
python scripts/check_secrets.py
```

### Advanced Security Features

**1. Startup Validation** - The application validates API keys at startup and warns if placeholders are used:
```
⚠️  API_KEY not set or using placeholder value - API data sources may not work
```

**2. Pre-commit Hook** - Automatically scans for secrets before commits:
```bash
# Install pre-commit hook
cp scripts/check_secrets.py .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

**3. CI/CD Secret Detection** - GitHub Actions workflow automatically scans on every push:
- Detects hardcoded API keys and secrets
- Verifies `.env` is gitignored
- Fails builds if secrets found

For complete security documentation, see [SECURITY.md](file:///f:/kasparro-etl/SECURITY.md).

**Result:** Zero secrets in version control, production-grade security! 🔒

## 📊 Monitoring

### Check ETL Status
```bash
# Via API
Invoke-RestMethod http://localhost:8000/health

# Via database
docker compose exec postgres psql -U etl_user -d etl_db -c "SELECT * FROM etl_checkpoints;"
```

### View Recent ETL Runs
```bash
docker compose exec postgres psql -U etl_user -d etl_db -c "SELECT run_id, source_name, status, started_at, records_loaded FROM etl_runs ORDER BY started_at DESC LIMIT 10;"
```

## 🎯 Key Features

- ✅ **Idempotent Operations** - Safe to re-run without duplicates
- ✅ **Resume on Failure** - Continues from last checkpoint
- ✅ **Transaction Safety** - Proper rollback handling
- ✅ **Multi-Source Support** - API, CSV, RSS feeds
- ✅ **Automated Scheduling** - Background ETL jobs
- ✅ **RESTful API** - Query and filter ingested data
- ✅ **Health Monitoring** - Track ETL status per source
- ✅ **Production Ready** - Docker, async operations, error handling

## 🧪 Smoke Test

Follow these steps to verify the system is working correctly:

### 1. Verify System Health
```bash
Invoke-RestMethod http://localhost:8000/health
```
**Expected:** `database_connected = True`, `total_sources >= 2`

### 2. Run ETL Manually
```bash
docker compose exec api python scripts/run_etl.py
```
**Expected:** `"ETL completed"` message, no errors

### 3. Verify Data Loaded
```bash
Invoke-RestMethod http://localhost:8000/data
```
**Expected:** Records from CoinGecko API and CSV sources

### 4. Check Statistics
```bash
Invoke-RestMethod http://localhost:8000/stats
```
**Expected:** 
- `total_records > 0`
- `success_rate = 100.0` for successful sources
- `recent_runs` showing completed ETL executions

### 5. Verify Idempotency
```bash
# Run ETL again
docker compose exec api python scripts/run_etl.py

# Check total_records unchanged
Invoke-RestMethod http://localhost:8000/stats
```
**Expected:** `total_records` stays the same (no duplicates)

### 6. Test Filtering
```bash
Invoke-RestMethod "http://localhost:8000/data?source_type=api&page_size=5"
```
**Expected:** Only API source records returned

## ⚠️ Known Limitations

- **Test Database Connection** - Tests configured for `localhost:5432` don't work inside Docker containers (need `postgres:5432`)
- **Scheduler Configuration** - Schedule interval (30 min) is hardcoded in `scheduler.py`, not configurable via environment variables
- **No Authentication** - API endpoints are publicly accessible without authentication
- **Single Database** - No replication or high-availability setup
- **Manual Cloud Deployment** - Requires manual setup on cloud platforms (see deployment guide below)
- **Limited Error Notifications** - No email/Slack alerts on ETL failures (only logged)
- **RSS Extractor Async Issue** - RSS feed extractor has async compatibility issues with feedparser library in current implementation (API and CSV sources fully functional)

## ☁️ Cloud Deployment

**The system is production-ready and deployed on AWS EC2. Follow these steps to deploy to any cloud provider.**

### Prerequisites
- Cloud account (AWS/GCP/Azure)
- Docker installed on cloud instance
- Public IP or load balancer

### General Steps (Any Cloud Provider)

1. **Provision VM/Container Instance**
   - Minimum: 2 vCPU, 4GB RAM
   - Open ports: 8000 (API), 5432 (PostgreSQL)

2. **Install Dependencies**
   ```bash
   # Install Docker & Docker Compose
   curl -fsSL https://get.docker.com -o get-docker.sh
   sh get-docker.sh
   
   # Install Docker Compose
   sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
   sudo chmod +x /usr/local/bin/docker-compose
   ```

3. **Deploy Application**
   ```bash
   # Clone repository
   git clone <your-repo-url>
   cd kasparro-etl
   
   # Configure environment
   cp .env.example .env
   # Edit .env with production values
   
   # Start services
   docker compose up -d --build
   
   # Initialize database
   docker compose exec postgres psql -U etl_user -d etl_db -c "CREATE EXTENSION IF NOT EXISTS btree_gin;"
   docker compose exec api python scripts/init_db.py
   ```

4. **Verify Deployment**
   ```bash
   curl http://<your-public-ip>:8000/health
   ```

### AWS EC2 Specific
```bash
# After launching Ubuntu EC2 instance
# Security Group: Allow inbound 8000, 5432

# SSH into instance
ssh -i your-key.pem ubuntu@<ec2-public-ip>

# Follow general steps above

# Optional: Setup CloudWatch for logs
aws logs create-log-group --log-group-name /ecs/kasparro-etl
```

### GCP Compute Engine
```bash
# Create VM instance
gcloud compute instances create kasparro-etl \
  --machine-type=e2-medium \
  --image-family=ubuntu-2004-lts \
  --image-project=ubuntu-os-cloud

# SSH and follow general steps
gcloud compute ssh kasparro-etl
```

### Automated Scheduling in Cloud
The APScheduler runs automatically inside the container. No additional cloud scheduler needed!

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `docker compose exec api pytest`
5. Submit a pull request

## 📝 License

MIT License - see LICENSE file for details

## 🆘 Support

For issues and questions:
- Check the troubleshooting section above
- Review logs: `docker compose logs api`
- Open an issue on GitHub

---

**Built with:** FastAPI • PostgreSQL • SQLAlchemy • Pydantic • Docker • APScheduler
