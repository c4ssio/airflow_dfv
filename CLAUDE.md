## Project Overview

This repo contains an Airflow (CeleryExecutor) pipeline that downloads SEC EDGAR
company data, ingests it into PostgreSQL, fetches daily ticker prices (Yahoo Finance),
and validates the load. The primary entry point is the DAG in `dags/sec_scraper.py`.

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
│   │   └── fetch_ticker_prices.py
│   ├── postgres/
│   │   ├── helpers.py              # Connection, NDJSON loading, upsert helpers
│   │   ├── deploy_migrations.py    # Migration runner (--dry-run, --verbose, --rollback)
│   │   ├── migrations/             # SQL DDL files (YYYYMMDDHHMM__description.sql)
│   │   └── README.md
│   └── tests/
│       ├── conftest.py                 # Stubs for airflow/yfinance/psycopg2 (host testing)
│       ├── test_common.py              # load_settings, make_session, rate_limit, get_json
│       ├── test_common_converters.py   # pad_cik, validators, NDJSON converters
│       ├── test_fetch_company_ciks.py  # fetch_company_ciks
│       ├── test_fetch_and_store_companies.py  # process_single_company, estimate_results_size_mb
│       ├── test_fetch_ticker_prices.py # PriceBar, _to_float/int, Stooq CSV parsing
│       ├── test_ingest_to_postgres.py  # _discover_cik_dirs, _build_ndjson_for_cik, ingest_ciks
│       ├── test_postgres_helpers.py    # get_postgres_config, write_ndjson_file
│       ├── test_storage.py             # s3_key, write_bytes, metadata, find_existing_data
│       └── test_validate_postgres_ingestion.py  # All 5 validation scenarios
├── infra/                          # Terraform IaC for AWS deployment
│   ├── main.tf                     # Provider config (AWS ~> 5.0, random ~> 3.0)
│   ├── variables.tf                # Input variables (region, instance sizes, CIDR, etc.)
│   ├── outputs.tf                  # ALB URL, RDS/Redis endpoints, ECR repo URL
│   ├── vpc.tf                      # VPC, 2 public + 2 private subnets, IGW, NAT, routes
│   ├── security_groups.tf          # 6 SGs: ALB, ECS, RDS, Redis, EFS, Jumpbox
│   ├── ecr.tf                      # ECR repository + lifecycle policy
│   ├── rds.tf                      # RDS PostgreSQL 15 (db.t4g.micro), random passwords
│   ├── elasticache.tf              # ElastiCache Redis 7 (cache.t4g.micro)
│   ├── efs.tf                      # EFS + access points for data/logs (UID 50000)
│   ├── secrets.tf                  # Secrets Manager for DB + Airflow admin credentials
│   ├── iam.tf                      # Execution role + task role, CloudWatch log group
│   ├── alb.tf                      # ALB on port 8080 with health checks
│   ├── ecs.tf                      # ECS Fargate cluster, 6 task defs, 5 services
│   ├── jumpbox.tf                  # Jumpbox EC2 in public subnet (terraform, ECS exec)
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
│       ├── build_and_push.sh       # Build Docker image and push to ECR
│       ├── deploy.sh               # Full deploy: terraform apply → build → init → redeploy
│       ├── teardown.sh             # Scale down ECS + terraform destroy
│       ├── stop_stack.sh           # Stop stack to save costs (ECS→0, RDS stop, EC2 stop)
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
5. Fetch daily ticker prices for tracked tickers (from `submissions_ticker_mapping`) via Yahoo Finance (yfinance) and upsert into `sec_raw.ticker_prices_daily`.

## DAG Task Sequence

```
get_company_ciks → fetch_and_store_companies → ingest_to_postgres → validate_postgres_ingestion → fetch_ticker_prices → summarize
```

- **Schedule:** `0 6 * * *` (daily at 06:00), `catchup=False`, `max_active_runs=1`.
- **Default retries:** 2 with 2-minute delay.
- `fetch_ticker_prices` uses the Airflow logical date (`ds`) so backfills pull historical prices.

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
| `SEC_PRICE_DATE` | No | today | Override price fetch date |
| `SEC_MAX_TICKERS_PER_RUN` | No | unlimited | Limit tickers for price fetch |
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

# On ECS Fargate — run as a one-off task (recommended, captures logs in CloudWatch)
aws ecs run-task --cluster sec-scraper-cluster \
  --task-definition sec-scraper-worker --launch-type FARGATE \
  --network-configuration '{
    "awsvpcConfiguration": {
      "subnets": ["<private-subnet-1>", "<private-subnet-2>"],
      "securityGroups": ["<ecs-sg>"],
      "assignPublicIp": "DISABLED"
    }
  }' \
  --overrides '{
    "containerOverrides": [{
      "name": "worker",
      "command": ["python", "/opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py", "--verbose"]
    }]
  }'
# Then check CloudWatch logs at /ecs/sec-scraper, stream prefix worker/worker/<task-id>

# On ECS Fargate — interactive (requires SSM Session Manager plugin installed locally)
TASK_ID=$(aws ecs list-tasks --cluster sec-scraper-cluster --service-name sec-scraper-worker --query 'taskArns[0]' --output text)
aws ecs execute-command --cluster sec-scraper-cluster --task "$TASK_ID" \
  --container worker --interactive \
  --command "python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py"
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

### Type Checking

Pyright is configured in `pyrightconfig.json` (basic mode, Python 3.11). Includes `dags/`, `plugins/`, `scripts/` with `plugins/` as an extra path.

## Code Style Guidelines

- **Avoid meaningless qualifiers**: Don't use "all" in function/method names (e.g., use `fetch_companies` not `fetch_all_companies`, `ingest_ciks` not `ingest_all_ciks`).
- **Keep DAG files thin**: Extract business logic to `plugins/scripts/sec_scraper/` modules. DAG files should contain only task wrappers and orchestration.
- **Testability**: Functions should accept dependencies as parameters (dependency injection) rather than importing them directly, making them easier to unit test.
- **Import convention**: In the DAG file, import task functions inside the `@task` wrapper to avoid import-time side effects. Module-level imports are fine for `common.py` utilities.
- **Settings**: All configuration flows through the `Settings` dataclass in `common.py`. Never read `os.environ` directly in task modules—use `Settings` fields instead.
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

The `sec_data` database is auto-created via `scripts/init-sec-db.sql` (mounted into Postgres `docker-entrypoint-initdb.d`).

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
| Jumpbox | EC2 t3.micro in public subnet | SSH + SSM access |

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

### ECS Postgres Configuration

On ECS, database credentials are passed as environment variables (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SCHEMA`) directly in the task definitions. The `get_postgres_config()` function in `helpers.py` checks for `POSTGRES_HOST` first and uses env vars when set, falling back to `postgres.yaml` only for local Docker Compose.

### Deploying to AWS

```bash
# 1. Configure variables
cd infra
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set allowed_cidr to your IP/32

# 2. Full deploy (init → build → push → start services)
./scripts/aws/deploy.sh

# 3. Or step-by-step:
cd infra && terraform init && terraform apply
./scripts/aws/build_and_push.sh    # Build image, push to ECR
./scripts/aws/deploy.sh --skip-build  # Run init + redeploy services
```

### Tearing Down

```bash
./scripts/aws/teardown.sh --yes
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

### How It Works

The api-server container uses an entrypoint wrapper that writes the password file before starting Airflow:
```
echo '{"admin": "$AIRFLOW_ADMIN_PASSWORD"}' > /opt/airflow/simple_auth_manager_passwords.json.generated
exec airflow api-server
```

The `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` env var (set to `admin:admin`) defines the user and role. The `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` env var points to the generated file. Only the api-server needs this — other services don't serve the UI.

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

A t3.micro EC2 instance in a public subnet for admin tasks (terraform, ECS exec, database access). Managed in `infra/jumpbox.tf`. Uses an **Elastic IP** so the address persists across stop/start cycles — no need to update security groups or SSH configs when restarting.

### What's Installed

- Terraform
- AWS CLI (via instance profile with AdministratorAccess)
- Docker
- SSM Session Manager plugin
- Git

### Access

```bash
# SSH (requires the remote_cursor_key private key)
ssh -i ~/.ssh/remote_cursor_key ec2-user@$(cd infra && terraform output -raw jumpbox_public_ip)

# SSM Session Manager (no SSH key needed, requires AWS CLI locally)
aws ssm start-session --target $(cd infra && terraform output -raw jumpbox_instance_id)
```

### Running Terraform from the Jumpbox

```bash
ssh ec2-user@<jumpbox-ip>
cd ~/projects/airflow_dfv/infra
terraform init
terraform apply
```

### Triggering DAG Runs from the Jumpbox

```bash
# Via Airflow CLI (one-off ECS task)
aws ecs run-task --cluster sec-scraper-cluster \
  --task-definition sec-scraper-worker --launch-type FARGATE \
  --network-configuration '{
    "awsvpcConfiguration": {
      "subnets": ["<private-subnet-1>", "<private-subnet-2>"],
      "securityGroups": ["<ecs-sg>"],
      "assignPublicIp": "DISABLED"
    }
  }' \
  --overrides '{
    "containerOverrides": [{
      "name": "worker",
      "command": ["bash", "-c", "airflow dags unpause sec_scraper && airflow dags trigger sec_scraper"]
    }]
  }'

# Via ECS exec (interactive, on running worker)
TASK_ID=$(aws ecs list-tasks --cluster sec-scraper-cluster \
  --service-name sec-scraper-worker --query 'taskArns[0]' --output text)
aws ecs execute-command --cluster sec-scraper-cluster --task "$TASK_ID" \
  --container worker --interactive --command bash
```

### Redeploying Services from the Jumpbox

```bash
# After code changes — rebuild image and push
cd ~/projects/airflow_dfv
./scripts/aws/build_and_push.sh

# Force ECS to pull the new image
aws ecs update-service --cluster sec-scraper-cluster \
  --service sec-scraper-worker --force-new-deployment
# Repeat for other services as needed
```

## Operational Notes for Claude Code Sessions

### AWS Credentials

When connecting to the AWS account from a Claude Code session (without jumpbox access):

- **AWS CLI is not pre-installed** — install with `pip install awscli`.
- Set credentials via env vars: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION=us-east-1`.
- **Cannot run `terraform apply`** — use the jumpbox for terraform operations.
- **Cannot use `aws ecs execute-command`** — requires SSM Session Manager plugin (not available in Claude Code sandbox).

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

### Updating ECS Task Definitions Without Terraform

When you need to update a running service's config without terraform (e.g., adding env vars):

1. `aws ecs describe-task-definition --task-definition <name>` → save JSON
2. Modify the container definition (add env vars, change command, etc.)
3. Remove non-registerable fields (`taskDefinitionArn`, `revision`, `status`, `requiresAttributes`, `compatibilities`, `registeredAt`, `registeredBy`, `enableFaultInjection`)
4. `aws ecs register-task-definition --cli-input-json file://modified.json`
5. `aws ecs update-service --cluster sec-scraper-cluster --service <name> --task-definition <name>:<new-revision> --force-new-deployment`

### Key Resource IDs (Current Deployment)

| Resource | Value |
|----------|-------|
| VPC | `vpc-068394bf0f50bb05b` |
| Private subnets | `subnet-0b9d77ddeb71d38fc`, `subnet-0bc236aee1170c5f5` |
| Public subnets | `subnet-048aef579a62e1f3b`, `subnet-0870391697fc216c4` |
| ECS security group | `sg-04d71ebe55272f655` |
| ALB security group | `sg-041181b4ade7d74f7` |
| Jumpbox security group | `sg-02704825b173677e0` |
| Jumpbox instance | `i-08608d20f790c623f` |
| Jumpbox Elastic IP | `44.198.7.177` (`eipalloc-0f95920bc202f2538`) |
| ALB DNS | `sec-scraper-alb-2104629405.us-east-1.elb.amazonaws.com` |
| CloudWatch log group | `/ecs/sec-scraper` |

### Airflow 3.x CLI Differences

- `airflow dags list-runs` syntax changed from Airflow 2. Use `airflow dags list-runs <dag_id>` (no `-d` flag).
- Health endpoint moved to `/api/v2/monitor/health` (not `/health`).
- API uses JWT tokens via `/auth/token`, not basic auth.
- DAGs are paused at creation on ECS (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`). Always `unpause` before triggering.

## On-Demand Stack (Cost Management)

The stack can be stopped and started on demand to save costs when not in use.

### Stop the Stack

```bash
./scripts/aws/stop_stack.sh            # Stop ECS, RDS, jumpbox
./scripts/aws/stop_stack.sh --delete-redis  # Also delete Redis (saves ~$12/mo)
```

**What gets stopped (free when stopped):**
- ECS Fargate services → scaled to 0
- RDS PostgreSQL → stopped (note: AWS auto-restarts after 7 days)
- Jumpbox EC2 → stopped (Elastic IP retained, same IP on restart)

**What keeps running (baseline cost when stopped):**

| Resource | Monthly Cost | Notes |
|----------|-------------|-------|
| NAT Gateway | ~$32 | Required for private subnet internet |
| ALB | ~$16 | Keeps DNS stable |
| ElastiCache Redis | ~$12 | Cannot be stopped; use `--delete-redis` to remove |
| EIP | $0 | Free when attached (even to stopped instance) |
| EFS | ~$0.30/GB | Minimal unless storing lots of data |

### Start the Stack

```bash
./scripts/aws/start_stack.sh              # Start everything back up
./scripts/aws/start_stack.sh --create-redis  # Also recreate Redis if deleted
```

The start script waits for RDS to become available before scaling up ECS services. Full startup takes ~5 minutes.

### Full Teardown (Destroy Everything)

To completely destroy all resources (irreversible):

```bash
./scripts/aws/teardown.sh --yes
```

This runs `terraform destroy` and removes all AWS resources including data.

## Notes for Changes

- **Secrets**: Do not commit `config/secrets.env` or `config/postgres.yaml`. The entire `config/` directory is gitignored except `*.example` files.
- **Local-only data**: `data/` (raw JSON) and `logs/` (Airflow logs) are gitignored.
- **Schema changes**: If you change PostgreSQL schemas, add a new migration SQL file and update any NDJSON conversion logic in `common.py` or `ingest_to_postgres.py`.
- **New tasks**: Create the task module in `plugins/scripts/sec_scraper/tasks/`, then add a thin `@task` wrapper in the DAG file.
- **Diagnostics output**: Scripts that produce diagnostic output write to `diagnostics/` (also gitignored).
