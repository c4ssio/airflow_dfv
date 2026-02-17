## Project Overview

This repo contains an Airflow (CeleryExecutor) pipeline that downloads SEC EDGAR
company data, ingests it into PostgreSQL, fetches daily ticker prices (Yahoo Finance),
computes Berkshire-style valuation scores, and validates the load. The primary entry
point is the DAG in `dags/sec_scraper.py`.

**Stack:** Python 3.11, Apache Airflow 3.1.5 (CeleryExecutor), PostgreSQL 15, Redis 7.

## Directory Structure

```
airflow_dfv/
├── dags/
│   ├── sec_scraper.py              # Airflow DAG — thin task wrappers only
│   └── sec_scraper.md              # DAG documentation
├── plugins/scripts/sec_scraper/    # All business logic lives here
│   ├── common.py                   # Settings, HTTP helpers, rate limiter, data converters
│   ├── storage.py                  # Local/S3 read/write helpers
│   ├── tasks/
│   │   ├── fetch_company_ciks.py
│   │   ├── fetch_and_store_companies.py
│   │   ├── ingest_to_postgres.py
│   │   ├── validate_postgres_ingestion.py
│   │   ├── fetch_ticker_prices.py
│   │   └── company_valuation_score.py  # Berkshire-style cheapness/quality scoring
│   ├── postgres/
│   │   ├── helpers.py              # Connection, NDJSON loading, upsert helpers
│   │   ├── deploy_migrations.py    # Migration runner (--dry-run, --verbose, --rollback)
│   │   ├── init_databases.py       # Creates sec_data DB + runs migrations (used by ECS init)
│   │   ├── run_log.py              # Pipeline run tracking (record_completed_run, get_last_successful_run_date)
│   │   ├── migrations/             # SQL DDL files (YYYYMMDDHHMM__description.sql)
│   │   └── README.md
│   └── tests/
│       ├── conftest.py                         # Stubs for airflow/yfinance/psycopg2 (host testing)
│       ├── test_common.py                      # load_settings, make_session, rate_limit, get_json
│       ├── test_common_converters.py           # pad_cik, validators, NDJSON converters
│       ├── test_fetch_company_ciks.py          # fetch_company_ciks
│       ├── test_fetch_and_store_companies.py   # process_single_company, estimate_results_size_mb
│       ├── test_fetch_ticker_prices.py         # PriceBar, _to_float/int, Stooq CSV parsing
│       ├── test_ingest_to_postgres.py          # _discover_cik_dirs, _build_ndjson_for_cik, ingest_ciks
│       ├── test_postgres_helpers.py            # get_postgres_config, write_ndjson_file
│       ├── test_storage.py                     # s3_key, write_bytes, metadata, find_existing_data
│       ├── test_validate_postgres_ingestion.py # All 5 validation scenarios
│       ├── test_company_valuation_score.py     # Valuation score computation
│       ├── test_integration_valuation_score.py # Integration test with ephemeral Postgres
│       ├── test_deploy_migrations.py           # Migration runner
│       └── test_run_log.py                     # Pipeline run log recording
├── infra/                          # Terraform IaC for AWS deployment
│   ├── main.tf                     # Provider config (AWS ~> 5.0, random ~> 3.0)
│   ├── variables.tf                # Input variables (region, instance sizes, CIDR, etc.)
│   ├── outputs.tf                  # ALB URL, RDS/Redis endpoints, ECR repo URL
│   ├── vpc.tf                      # VPC, 2 public + 2 private subnets, IGW, NAT, routes
│   ├── security_groups.tf          # 5 SGs: ALB, ECS, RDS, Redis, EFS
│   ├── ecr.tf                      # ECR repository + lifecycle policy
│   ├── rds.tf                      # RDS PostgreSQL 15 (db.t4g.micro), random passwords
│   ├── elasticache.tf              # ElastiCache Redis 7 (cache.t4g.micro)
│   ├── efs.tf                      # EFS + access points for data/logs (UID 50000)
│   ├── secrets.tf                  # Secrets Manager for DB + Airflow admin credentials
│   ├── iam.tf                      # Execution role + task role, CloudWatch log group
│   ├── alb.tf                      # ALB on port 8080 with health checks
│   ├── ecs.tf                      # ECS Fargate cluster, 6 task defs, 5 services
│   └── terraform.tfvars.example    # Example variable values
├── scripts/
│   ├── setup_venv.sh               # Create Python venv on the host
│   ├── run_with_venv.sh            # Run commands inside the venv
│   ├── check_airflow.sh            # Airflow diagnostics → diagnostics/
│   ├── restart_airflow_services.sh
│   ├── check_worker_memory.sh
│   ├── monitor_worker_memory.sh
│   ├── cleanup_removed_tasks.py    # Remove stale Airflow task metadata
│   ├── init-sec-db.sql             # Creates sec_data DB on first Postgres startup
│   ├── adhoc/                      # One-off diagnostics (SQL, Python)
│   └── aws/                        # AWS deployment scripts
│       ├── create_jumpbox.sh       # Create standalone jumpbox EC2 (AWS CLI only, no Terraform)
│       ├── destroy_jumpbox.sh      # Destroy jumpbox EC2
│       ├── build_and_push.sh       # Build Docker image and push to ECR
│       ├── deploy.sh               # Full deploy: terraform apply → build → init → redeploy
│       ├── teardown.sh             # Scale down ECS + terraform destroy
│       ├── stop_stack.sh           # Stop stack to save costs (ECS→0, RDS stop)
│       └── start_stack.sh          # Start stack back up (reverse of stop)
├── config/                         # gitignored — secrets.env, postgres.yaml
├── compose.yaml                    # Docker Compose (all services)
├── Dockerfile                      # Extends apache/airflow:3.1.5
├── requirements.txt                # Python dependencies
├── pytest.ini                      # pytest configuration
├── pyrightconfig.json              # Pyright type checking (basic mode)
└── mermaid.md                      # Architecture diagram
```

## Core Data Flow

1. Download SEC `company_tickers.json` to enumerate CIKs.
2. For each CIK:
   - Always fetch `submissions/CIK*.json`.
   - Fetch `companyfacts/CIK*.json` only when new filings are detected.
   - Store raw JSON locally (`/opt/airflow/data/sec_raw`) or to S3.
   - Write `metadata.json` per CIK to support incremental updates.
3. Convert JSON to NDJSON and load into PostgreSQL `sec_raw` tables (submissions, companyfacts_metadata, companyfacts_facts, metric_metadata). Ingestion skips CIKs already present for the current ingest date (idempotent).
4. Run data integrity validations (row counts, NULL checks, PK uniqueness, type checks, array alignment).
5. Fetch daily ticker prices for tracked tickers (from `submissions_ticker_mapping`) via Yahoo Finance (yfinance) and upsert into `sec_raw.ticker_prices_daily`. Supports backfill mode — queries DB for most recent price_date and fills the gap through today.
6. Compute Berkshire-style valuation scores combining cheapness (55%) and quality (45%) factors. Each factor scored 0-100, combined into a composite grade (A-F). Stored in `sec_raw.company_valuation_scores`.
7. Record successful run in `pipeline_run_log` for backfill tracking.

## DAG Task Sequence

```
get_company_ciks → fetch_and_store_companies → ingest_to_postgres → validate_postgres_ingestion → fetch_ticker_prices → score_company_valuations → summarize
```

- **Schedule:** `None` (trigger-only; was previously `0 6 * * *`).
- **Default retries:** 2 with 2-minute delay.
- `fetch_ticker_prices` uses backfill mode: queries DB for last price_date and fills through today (or the Airflow logical date `ds`).
- `score_company_valuations` uses the Airflow logical date (`ds`) for the score date.
- `summarize` records the completed run to `pipeline_run_log`.

## Database Schema (sec_raw)

**Database:** `sec_data` | **Schema:** `sec_raw`

### Tables
| Table | Primary Key | Purpose |
|-------|-------------|---------|
| `submissions` | (cik, ingest_date) | Company metadata; JSONB arrays for tickers, exchanges, filings |
| `companyfacts_metadata` | (cik, ingest_date) | Per-company facts metadata |
| `companyfacts_facts` | (cik, ingest_date, taxonomy, metric_name, unit, period_end, accession_number) | Normalized financial metrics (no label/description columns) |
| `metric_metadata` | (taxonomy, metric_name) | Canonical metric reference; labels and descriptions live here |
| `ticker_prices_daily` | (ticker, price_date) | Daily OHLCV; source e.g. "yahoo" |
| `company_valuation_scores` | (cik, ticker, score_date) | Berkshire-style valuation scores — market data, fundamentals, ratios, component scores, composite grade |
| `pipeline_run_log` | (run_id) | Tracks DAG runs with start/end times, status, and summary JSONB |
| `schema_migrations` | (migration_name) | Migration tracking with MD5 checksums |

### Views
- `companyfacts_facts_full` — facts joined with metric_metadata (label, description, abbreviation, category).
- `companyfacts_facts_with_abbrev` — backward-compatible alias for `companyfacts_facts_full`.
- `submissions_ticker_mapping` — expands JSONB ticker/exchange arrays into rows with `is_primary_ticker`, `is_likely_common_stock`, `share_class_indicator`.

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SEC_USER_AGENT` | Yes | — | Email-based identifier required by SEC |
| `SEC_REQUESTS_PER_SECOND` | No | `5` | Rate limit for SEC API |
| `SEC_TIMEOUT_SECONDS` | No | `30` | HTTP request timeout |
| `SEC_MAX_CIKS_PER_RUN` | No | `250` | Max companies per DAG run |
| `SEC_START_CIK` | No | `""` | Start from a specific CIK |
| `SEC_LOCAL_DIR` | No | `/tmp/sec_raw` | Local storage path |
| `SEC_S3_BUCKET` | No | `""` | S3 bucket (empty = local only) |
| `SEC_S3_PREFIX` | No | `sec_raw` | S3 key prefix |
| `SEC_INGEST_TEST_CIK` | No | `""` | Test with a single CIK |
| `SEC_INGEST_MAX_CIKS` | No | `0` | Limit CIKs during ingestion (0 = no limit) |
| `SEC_PRICE_DATE` | No | today | Override price fetch date (also used as valuation score date) |
| `SEC_MAX_TICKERS_PER_RUN` | No | `max_ciks` | Limit tickers for price fetch |
| `POSTGRES_CONFIG_PATH` | No | auto-detected | Path to `postgres.yaml` (local only) |
| `POSTGRES_HOST` | No | — | RDS host; when set, env vars override `postgres.yaml` |
| `POSTGRES_PORT` | No | `5432` | Database port (used when `POSTGRES_HOST` is set) |
| `POSTGRES_DB` | No | `sec_data` | Database name (used when `POSTGRES_HOST` is set) |
| `POSTGRES_USER` | No | `airflow` | Database user (used when `POSTGRES_HOST` is set) |
| `POSTGRES_PASSWORD` | No | `airflow` | Database password (used when `POSTGRES_HOST` is set) |
| `POSTGRES_SCHEMA` | No | `sec_raw` | Database schema (used when `POSTGRES_HOST` is set) |

### Postgres Config (`config/postgres.yaml`, gitignored)

```yaml
host: postgres
port: 5432
database: sec_data
user: airflow
password: airflow
schema: sec_raw
```

## Development Setup

### Starting the Stack

```bash
docker compose build          # Build Airflow image with dependencies
docker compose up -d          # Start all services
```

Airflow UI: http://localhost:8080

### Installing New Python Dependencies

Add to `requirements.txt`, then rebuild:

```bash
docker compose build && docker compose up -d
```

### Running Migrations

```bash
# Inside Docker (recommended)
docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py

# Preview only
docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py --dry-run

# From host with venv
./scripts/run_with_venv.sh python plugins/scripts/sec_scraper/postgres/deploy_migrations.py
```

### Migration File Format

Files must follow `YYYYMMDDHHMM__description.sql` naming. Migrations run in chronological order by filename prefix. Optional rollback files: `YYYYMMDDHHMM__description__rollback.sql`.

## Testing

### Running Tests

```bash
# From project root (host, with venv)
./scripts/run_with_venv.sh python -m pytest

# Inside Docker
docker compose exec airflow-worker python -m pytest /opt/airflow/plugins/scripts/sec_scraper/tests/

# Single test file
./scripts/run_with_venv.sh python -m pytest plugins/scripts/sec_scraper/tests/test_common_converters.py -v
```

### Test Conventions

- Tests live in `plugins/scripts/sec_scraper/tests/`.
- pytest is configured via `pytest.ini` (testpaths, pythonpath).
- Use dependency injection: pass `Settings` and `requests.Session` as parameters so tests can supply fakes (see `DummySession` in `test_fetch_company_ciks.py`).
- Test function names should describe the behavior being tested: `test_<function>_<scenario>`.
- **Host-side stubs**: `conftest.py` provides lightweight stubs for `airflow`, `yfinance`, and `psycopg2` so tests run on the host without the full Docker stack. Stubs are only registered when the real module is not installed.
- **DB-dependent functions**: Test via their callers using dependency injection and mock connections (e.g., `_FakeLoadFns` in `test_ingest_to_postgres.py`, `_FakeCursor`/`_FakeConn` in `test_validate_postgres_ingestion.py`).
- **New task tests**: When adding a new task module, add a corresponding `test_<module>.py` file. Use the `_make_settings()` helper pattern (see any existing test file) to create `Settings` instances with sensible defaults.
- **Integration tests**: `test_integration_valuation_score.py` tests against an ephemeral Postgres instance. These require a running database and are skipped when Postgres is unavailable.

### Type Checking

Pyright is configured in `pyrightconfig.json` (basic mode, Python 3.11). Includes `dags/`, `plugins/`, `scripts/` with `plugins/` as an extra path.

## Code Style Guidelines

- **Avoid meaningless qualifiers**: Don't use "all" in function/method names (e.g., use `fetch_companies` not `fetch_all_companies`, `ingest_ciks` not `ingest_all_ciks`).
- **Keep DAG files thin**: Extract business logic to `plugins/scripts/sec_scraper/` modules. DAG files should contain only task wrappers and orchestration.
- **Testability**: Functions should accept dependencies as parameters (dependency injection) rather than importing them directly, making them easier to unit test.
- **Import convention**: In the DAG file, import task functions inside the `@task` wrapper to avoid import-time side effects. Module-level imports are fine for `common.py` utilities.
- **Settings**: All configuration flows through the `Settings` dataclass in `common.py`. Never read `os.environ` directly in task modules—use `Settings` fields instead. (Exception: `SEC_PRICE_DATE` and `SEC_MAX_TICKERS_PER_RUN` are read directly in `fetch_ticker_prices` and `company_valuation_score` because they are task-level overrides not in the Settings dataclass.)
- **CIK format**: CIKs are zero-padded to 10 digits via `pad_cik()` before storage or database operations.

## Docker Architecture

Services defined in `compose.yaml`:

| Service | Purpose |
|---------|---------|
| `postgres` | PostgreSQL 15 — Airflow metadata DB + `sec_data` DB |
| `redis` | Celery broker |
| `airflow-init` | One-shot: runs `airflow db migrate` |
| `airflow-api-server` | API/UI server (port 8080) |
| `airflow-scheduler` | Task scheduler |
| `airflow-dag-processor` | DAG parser |
| `airflow-worker` | Celery task executor (concurrency: 2) |
| `airflow-triggerer` | Event-based triggers |

Volumes mount `dags/`, `plugins/`, `data/`, `config/`, and `logs/` into containers at `/opt/airflow/`.

### Local vs ECS Code Strategy

The Dockerfile `COPY`s `dags/` and `plugins/` into the image so ECS Fargate containers have application code baked in. In local development, Docker Compose volume mounts shadow these baked-in paths, so edits are reflected immediately without rebuilding. When deploying to ECS, run `build_and_push.sh` to rebuild the image with the latest code.

The `sec_data` database is auto-created via `scripts/init-sec-db.sql` (mounted into Postgres `docker-entrypoint-initdb.d`) locally, and via `init_databases.py` on ECS.

## AWS Deployment (ECS Fargate)

The `infra/` directory contains Terraform configuration for deploying the full Airflow stack to AWS. All resources are tagged `Environment=ephemeral` for easy identification.

### AWS Architecture

| Component | AWS Service | Size |
|-----------|-------------|------|
| VPC | 2 public + 2 private subnets, NAT gateway | 10.0.0.0/16 |
| Database | RDS PostgreSQL 15 | db.t4g.micro (20 GB gp3) |
| Cache | ElastiCache Redis 7 | cache.t4g.micro |
| Compute | ECS Fargate (5 services) | See below |
| Storage | EFS (data + logs) | Bursting throughput |
| Load Balancer | ALB on port 8080 | — |
| Container Registry | ECR | Keep last 5 images |
| Secrets | Secrets Manager | DB + Airflow admin credentials |
| Logs | CloudWatch | 7-day retention |

### ECS Naming Conventions

All ECS resources are prefixed with the project name (`sec-scraper` by default):

| Resource | Name |
|----------|------|
| Cluster | `sec-scraper-cluster` |
| Services | `sec-scraper-worker`, `sec-scraper-api-server`, `sec-scraper-scheduler`, etc. |
| Containers | Named after their role: `worker`, `api-server`, `scheduler`, `dag-processor`, `triggerer`, `init` |
| CloudWatch log group | `/ecs/sec-scraper` |
| Log stream format | `<stream-prefix>/<container-name>/<task-id>` (e.g., `worker/worker/abc123`) |

### ECS Services

| Service | CPU | Memory | Notes |
|---------|-----|--------|-------|
| api-server | 512 | 1024 | Behind ALB, health-checked |
| scheduler | 256 | 512 | |
| dag-processor | 256 | 512 | |
| worker | 1024 | 2048 | Celery executor, `execute-command` enabled |
| triggerer | 256 | 512 | |
| init (one-shot) | 512 | 1024 | Creates sec_data DB + airflow db migrate |

### Deploying to AWS

The jumpbox is managed separately from the main infrastructure (no circular Terraform dependency).

```bash
# 1. Create jumpbox (locally, needs only AWS CLI)
./scripts/aws/create_jumpbox.sh

# 2. SSH to jumpbox
ssh -i ~/.ssh/sec-scraper-jumpbox-key.pem ec2-user@<ip-from-output>

# 3. On the jumpbox: clone repo and deploy
git clone <your-repo-url> ~/projects/airflow_dfv
cd ~/projects/airflow_dfv/infra
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set allowed_cidr to your IP/32
cd .. && ./scripts/aws/deploy.sh
```

### Tearing Down

```bash
# 1. On the jumpbox: destroy Terraform resources
./scripts/aws/teardown.sh --yes

# 2. Locally: destroy the jumpbox itself
./scripts/aws/destroy_jumpbox.sh
```

### Terraform State

State is stored locally in `infra/terraform.tfstate` (gitignored). For team use, configure an S3 backend in `main.tf`.

### Security Notes

- **No hardcoded secrets**: DB password is generated via `random_password` at apply time.
- **ALB access**: Restricted by `allowed_cidr` variable (default: `0.0.0.0/0` — set to your IP).
- **Private subnets**: RDS, Redis, EFS, and ECS tasks are in private subnets; only the ALB is public.
- **IAM least-privilege**: Execution role has ECR pull + Secrets Manager read; task role has EFS mount + SSM messages (for `execute-command`).
- **Never commit**: `infra/terraform.tfstate`, `infra/.terraform/`, or `infra/terraform.tfvars` (all gitignored).

## Airflow UI Access (ECS)

The ECS deployment uses Airflow 3's **Simple Auth Manager** for UI/API authentication.

- **Username:** `admin`
- **Password:** Auto-generated at deploy time via `random_password.airflow_admin` in Terraform.
- **Retrieve password:** `cd infra && terraform output -raw airflow_admin_password`
- **ALB URL:** Available via `terraform output alb_url` (format: `http://<alb-dns>:8080`)

### API Authentication (Airflow 3.x)

Airflow 3.x does **not** use basic auth for API calls. To authenticate programmatically:

1. Get a JWT token:
   ```bash
   curl -X POST http://<alb-url>:8080/auth/token \
     -H "Content-Type: application/json" \
     -d '{"username":"admin","password":"<password>"}'
   ```
2. Use the token in subsequent requests:
   ```bash
   curl -H "Authorization: Bearer <token>" http://<alb-url>:8080/api/v2/dags
   ```

### Health Check (no auth required)

```bash
curl http://<alb-url>:8080/api/v2/monitor/health
```

## Jumpbox

A t3.micro EC2 instance in the default VPC for running Terraform, Docker builds, and ECS exec. **Managed independently** via `scripts/aws/create_jumpbox.sh` (not Terraform), so there's no circular dependency.

### What's Installed

- Terraform, Docker, Git, SSM Session Manager plugin

### Lifecycle

```bash
# Create (locally, needs only AWS CLI)
./scripts/aws/create_jumpbox.sh

# SSH in
ssh -i ~/.ssh/sec-scraper-jumpbox-key.pem ec2-user@<ip>

# Destroy (locally)
./scripts/aws/destroy_jumpbox.sh
```

State is saved to `~/.sec-scraper-jumpbox.json` so the destroy script knows what to clean up.

## Operational Notes for Claude Code Sessions

### AWS CLI Setup

Install AWS CLI v2 and set credentials before doing anything AWS-related:

```bash
# Install AWS CLI v2 (required — v1 is missing features like --no-cli-pager)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli
rm -rf /tmp/awscliv2.zip /tmp/aws

# Set credentials (user will provide these)
export AWS_ACCESS_KEY_ID=<key>
export AWS_SECRET_ACCESS_KEY=<secret>
export AWS_DEFAULT_REGION=us-east-1
```

### What Claude Code Sessions CAN vs CANNOT Do

**Can do directly:**
- Query AWS resources (`aws ecs list-clusters`, `aws rds describe-db-instances`, etc.)
- Create/destroy the jumpbox (`create_jumpbox.sh` / `destroy_jumpbox.sh`)
- Trigger DAG runs via the Airflow REST API
- Update ECS task definitions and redeploy services
- Delete individual resources for teardown
- Run `aws ecs run-task` with command overrides

**Requires the jumpbox (SSH in first):**
- `terraform apply` / `terraform destroy`
- `aws ecs execute-command` (requires SSM Session Manager plugin)
- Building and pushing Docker images to ECR (requires Docker daemon)

### Running Airflow CLI Commands Remotely

Without jumpbox/SSM access, use `aws ecs run-task` with command overrides to run Airflow CLI commands. The container takes ~30-60s to start (Fargate provisioning). Check logs via CloudWatch:

```bash
# Run a command
aws ecs run-task --cluster sec-scraper-cluster \
  --task-definition sec-scraper-worker --launch-type FARGATE \
  --network-configuration '{"awsvpcConfiguration":{"subnets":["<subnet-1>","<subnet-2>"],"securityGroups":["<ecs-sg>"],"assignPublicIp":"DISABLED"}}' \
  --overrides '{"containerOverrides":[{"name":"worker","command":["bash","-c","<your-command>"]}]}'

# Check output (task ID from the run-task response)
aws logs get-log-events --log-group-name /ecs/sec-scraper \
  --log-stream-name worker/worker/<task-id> \
  --query 'events[].message' --output text
```

### Triggering a DAG Run from Claude Code

Use the Airflow REST API via the ALB (no SSM needed):

```bash
# 1. Get JWT token
TOKEN=$(curl -s -X POST http://<alb-url>:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"<password>"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# 2. Unpause the DAG (required — DAGs are paused at creation on ECS)
curl -s -X PATCH "http://<alb-url>:8080/api/v2/dags/sec_scraper" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"is_paused": false}'

# 3. Trigger the DAG
curl -s -X POST "http://<alb-url>:8080/api/v2/dags/sec_scraper/dagRuns" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}'

# 4. Monitor progress
curl -s "http://<alb-url>:8080/api/v2/dags/sec_scraper/dagRuns" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import sys, json
runs = json.load(sys.stdin).get('dag_runs', [])
for r in runs[:3]:
    print(f'{r[\"dag_run_id\"]}: {r[\"state\"]}')"
```

### Monitoring a DAG Run via API

```bash
# List task instances for a run
curl -s "http://<alb-url>:8080/api/v2/dags/sec_scraper/dagRuns/<run_id>/taskInstances" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "
import sys, json
tasks = json.load(sys.stdin).get('task_instances', [])
for t in tasks:
    print(f'{t[\"task_id\"]}: {t[\"state\"]}')"
```

### Updating ECS Task Definitions Without Terraform

When you need to update a running service's config without terraform (e.g., adding env vars):

1. `aws ecs describe-task-definition --task-definition <name>` → save JSON
2. Modify the container definition (add env vars, change command, etc.)
3. Remove non-registerable fields (`taskDefinitionArn`, `revision`, `status`, `requiresAttributes`, `compatibilities`, `registeredAt`, `registeredBy`, `enableFaultInjection`)
4. `aws ecs register-task-definition --cli-input-json file://modified.json`
5. `aws ecs update-service --cluster sec-scraper-cluster --service <name> --task-definition <name>:<new-revision> --force-new-deployment`

### Key Resource IDs (Current Deployment)

**No active deployment.** All AWS resources were torn down on 2026-02-17. To redeploy, create a jumpbox (`create_jumpbox.sh`), SSH in, and run `deploy.sh`. After deploying, update this section with the new resource IDs by running:

```bash
# From the jumpbox (after deploy.sh)
cd ~/projects/airflow_dfv/infra
echo "VPC: $(terraform output -raw vpc_id)"
echo "Private subnets: $(terraform output -json private_subnet_ids)"
echo "Public subnets: $(terraform output -json public_subnet_ids)"
echo "ALB DNS: $(terraform output -raw alb_url)"
```

### Airflow 3.x Gotchas

- `airflow dags list-runs` syntax: `airflow dags list-runs <dag_id>` (no `-d` flag).
- Health endpoint: `/api/v2/monitor/health` (not `/health`).
- API uses JWT tokens via `/auth/token`, not basic auth.
- DAGs are paused at creation on ECS (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`). Always `unpause` before triggering.
- **CloudWatch remote logging is broken**: Airflow 3.x has a known issue with the `apache-airflow-providers-amazon` CloudWatch task handler ([GH#52501](https://github.com/apache/airflow/issues/52501)). Do NOT set `AIRFLOW__LOGGING__REMOTE_LOGGING=True` with CloudWatch — it crashes all services.

### Airflow 3.x ECS-Critical Configuration

These env vars are **required** for Airflow 3.x CeleryExecutor on ECS Fargate. Without them, tasks will fail silently:

| Variable | Purpose |
|----------|---------|
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | URL workers use to reach the API server's Execution API (e.g., `http://<ALB>:8080/execution/`). Without this, workers can't communicate with the API server. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | **Must be identical across ALL services.** JWT signing key for internal Execution API auth. If each container generates its own, signature verification fails. |
| `AIRFLOW__CORE__FERNET_KEY` | Shared encryption key for Airflow connections/variables. Must be identical across all services. |
| `AIRFLOW__CELERY__OPERATION_TIMEOUT` | Default is 1s — too short for Fargate cold starts. Set to `10.0` to prevent a redis import race condition. |

The scheduler uses a custom entrypoint that pre-imports `redis.client` before starting, preventing the race condition where Celery's operation timeout interrupts the redis module import.

### Secrets Management

Sensitive values (Fernet key, JWT secret, DB passwords, admin password) are stored in **AWS Secrets Manager** at `sec-scraper/app-config`. The Terraform config generates these via `random_password` resources. On ECS, values are passed directly as environment variables in task definitions (not pulled at runtime from Secrets Manager — they're baked in at `register-task-definition` time).

## On-Demand Stack (Cost Management)

The stack can be stopped and started on demand to save costs when not in use.

### Stop the Stack

```bash
./scripts/aws/stop_stack.sh            # Stop ECS, RDS
./scripts/aws/stop_stack.sh --delete-redis  # Also delete Redis (saves ~$12/mo)
```

**What gets stopped (free when stopped):**
- ECS Fargate services → scaled to 0
- RDS PostgreSQL → stopped (note: AWS auto-restarts after 7 days)

**What keeps running (baseline cost when stopped):**

| Resource | Monthly Cost | Notes |
|----------|-------------|-------|
| NAT Gateway | ~$32 | Required for private subnet internet |
| ALB | ~$16 | Keeps DNS stable |
| ElastiCache Redis | ~$12 | Cannot be stopped; use `--delete-redis` to remove |
| EFS | ~$0.30/GB | Minimal unless storing lots of data |
| Jumpbox | ~$8 | Standalone EC2; destroy separately with `destroy_jumpbox.sh` |

### Start the Stack

```bash
./scripts/aws/start_stack.sh              # Start everything back up
./scripts/aws/start_stack.sh --create-redis  # Also recreate Redis if deleted
```

The start script waits for RDS to become available before scaling up ECS services. Full startup takes ~5 minutes.

## Notes for Changes

- **Secrets**: Do not commit `config/secrets.env` or `config/postgres.yaml`. The entire `config/` directory is gitignored except `*.example` files.
- **Local-only data**: `data/` (raw JSON) and `logs/` (Airflow logs) are gitignored.
- **Schema changes**: If you change PostgreSQL schemas, add a new migration SQL file and update any NDJSON conversion logic in `common.py` or `ingest_to_postgres.py`.
- **New tasks**: Create the task module in `plugins/scripts/sec_scraper/tasks/`, then add a thin `@task` wrapper in the DAG file.
- **Diagnostics output**: Scripts that produce diagnostic output write to `diagnostics/` (also gitignored).

## DAG Run Performance (Observed)

Typical timings for a full DAG run processing 250 CIKs on ECS (worker: 1024 CPU / 2048 MiB):

| Task | Duration |
|------|----------|
| `get_company_ciks` | ~10s |
| `fetch_and_store_companies` | ~5-8 min |
| `ingest_to_postgres` | ~25-30 min |
| `validate_postgres_ingestion` | ~10s |
| `fetch_ticker_prices` | ~2-3 min |
| `score_company_valuations` | ~1-2 min |
| `summarize` | ~5s |
| **Total** | **~35-45 min** |

`ingest_to_postgres` is the bottleneck — it converts raw JSON to NDJSON and bulk-loads into Postgres for each CIK sequentially.
