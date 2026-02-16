# AWS Deployment & DAG Backfill Plan

## Overview

Deploy the SEC scraper Airflow pipeline to AWS as ephemeral infrastructure: spin up, run the DAG, verify data, extract insights, and tear down. Amend the DAG to be trigger-only and backfill filings/prices since the last run.

This plan is split into two tracks that can proceed in parallel:

- **Track A** — DAG code changes (trigger-only scheduling, backfill logic)
- **Track B** — AWS infrastructure (Terraform IaC, deploy scripts)

---

## Track A: DAG & Business Logic Changes

### A1. Switch DAG to trigger-only scheduling

**File:** `dags/sec_scraper.py`

Change the DAG definition from `schedule="0 6 * * *"` to `schedule=None` (manual trigger only). This means the DAG only runs when explicitly triggered via the Airflow API or UI.

```python
with DAG(
    dag_id="sec_scraper",
    schedule=None,          # was: "0 6 * * *"
    catchup=False,
    max_active_runs=1,
    ...
)
```

### A2. Add backfill-since-last-run logic for price fetching

Currently `fetch_ticker_prices` fetches prices for a single date (`ds`). We need it to backfill all trading days since the last price date in the database.

**File:** `plugins/scripts/sec_scraper/tasks/fetch_ticker_prices.py`

Add a helper `_get_last_price_date(conn, schema)` that queries:
```sql
SELECT MAX(price_date) FROM {schema}.ticker_prices_daily
```

Modify `fetch_ticker_prices()`:
- Accept an optional `backfill_from` date parameter.
- When `backfill_from` is None, query the DB for the last price date and use the day after as `backfill_from`.
- Generate a list of business days from `backfill_from` through `price_date` (the target end date).
- Loop over each date and fetch/upsert prices for each.
- Return a summary with `dates_processed`, `total_stored`, etc.

**File:** `dags/sec_scraper.py`

Update the `fetch_ticker_prices` task wrapper to pass `backfill=True` so it fills the gap.

### A3. Add backfill-since-last-run logic for filings

Currently `fetch_and_store_companies` always processes the `SEC_MAX_CIKS_PER_RUN` batch. The incremental check (metadata-based) already handles only downloading companyfacts when new filings exist, so this works correctly for backfill as-is — running the full pipeline will re-check all CIKs and only download what's new.

No code change needed here — the existing incremental logic covers backfill for filings. We'll set `SEC_MAX_CIKS_PER_RUN` high enough (or 0 for "all") during the AWS run to ensure full coverage.

### A4. Add `SEC_MAX_CIKS_PER_RUN=0` means "all" support

**File:** `plugins/scripts/sec_scraper/tasks/fetch_company_ciks.py` (or wherever the CIK list is sliced)

Currently `max_ciks=250` caps the batch. Add logic so `0` means "process all CIKs" — this is important for a full ingest run.

### A5. Add `last_run_state` table/tracking (optional enhancement)

Add a new migration and small helper that records the last successful DAG run date. This lets the price backfill logic know where to start without relying on the presence of price data in the DB (handles first-run scenario).

**File:** `plugins/scripts/sec_scraper/postgres/migrations/YYYYMMDDHHMM__create_pipeline_run_log.sql`

```sql
CREATE TABLE IF NOT EXISTS sec_raw.pipeline_run_log (
    run_id       SERIAL PRIMARY KEY,
    run_date     DATE NOT NULL,
    started_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status       TEXT DEFAULT 'running',    -- running | success | failed
    summary      JSONB
);
```

The summarize task will INSERT a row at the start and UPDATE it at the end.

---

## Track B: AWS Infrastructure (Terraform)

### Architecture

```
┌──────────────────────────────────────────────────────────┐
│                        VPC                               │
│  ┌──────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ RDS      │  │ ElastiCache  │  │ ECS Fargate       │  │
│  │ Postgres │  │ Redis        │  │ ┌───────────────┐ │  │
│  │ 15       │  │ 7            │  │ │ api-server    │ │  │
│  │          │  │              │  │ │ scheduler     │ │  │
│  │ sec_data │  │ celery broker│  │ │ dag-processor │ │  │
│  │ airflow  │  │              │  │ │ worker        │ │  │
│  │          │  │              │  │ │ triggerer     │ │  │
│  └──────────┘  └──────────────┘  │ └───────────────┘ │  │
│                                  └───────────────────┘  │
│  ┌──────────┐  ┌──────────────┐                         │
│  │ EFS      │  │ ALB          │                         │
│  │ /data    │  │ :8080→api    │                         │
│  │ /logs    │  └──────────────┘                         │
│  └──────────┘                                           │
│  ┌──────────────┐  ┌─────────────┐                      │
│  │ Secrets Mgr  │  │ ECR         │                      │
│  │ postgres.yaml│  │ airflow img │                      │
│  │ secrets.env  │  └─────────────┘                      │
│  └──────────────┘                                       │
└──────────────────────────────────────────────────────────┘
```

**Why these choices:**

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Compute | ECS Fargate | No servers to manage; maps 1:1 to compose services; easy to spin down to zero |
| Database | RDS PostgreSQL 15 | Managed Postgres; `sec_data` + `airflow` DBs; `skip_final_snapshot` for teardown |
| Cache | ElastiCache Redis 7 | Managed Redis for Celery broker; single-node is sufficient |
| Storage | EFS | Shared filesystem across ECS tasks for `/data` and `/logs`; mirrors compose volumes |
| Secrets | AWS Secrets Manager | `postgres.yaml` and `secrets.env` values; injected as env vars into ECS tasks |
| Networking | ALB + public subnet | Exposes Airflow API on port 8080; restrict via security group to your IP |
| Container Registry | ECR | Push the Dockerfile image; referenced by ECS task definitions |

### B1. Create Terraform project structure

```
infra/
├── main.tf              # Provider, backend config
├── variables.tf         # Input variables (region, instance sizes, your IP, etc.)
├── outputs.tf           # ALB URL, RDS endpoint, ECR repo URL
├── vpc.tf               # VPC, subnets, IGW, NAT, route tables
├── security_groups.tf   # SGs for ALB, ECS, RDS, Redis, EFS
├── ecr.tf               # ECR repository for Airflow image
├── rds.tf               # RDS PostgreSQL 15 (multi-DB: airflow + sec_data)
├── elasticache.tf       # ElastiCache Redis 7
├── efs.tf               # EFS filesystem + mount targets + access point
├── secrets.tf           # Secrets Manager for DB creds, SEC_USER_AGENT, etc.
├── ecs.tf               # ECS cluster, task definitions, services
├── alb.tf               # ALB + target group + listener for API server
├── iam.tf               # Task execution role, task role (EFS, Secrets, ECR)
└── terraform.tfvars.example  # Example variable values
```

### B2. VPC & Networking (`vpc.tf`)

- 1 VPC with 2 AZs (for RDS multi-AZ if desired, but single-AZ is fine for ephemeral use)
- 2 public subnets (ALB, NAT gateway)
- 2 private subnets (ECS tasks, RDS, ElastiCache, EFS)
- 1 NAT gateway (ECS tasks need outbound internet for SEC API + Yahoo Finance)
- Internet gateway for ALB

### B3. RDS PostgreSQL (`rds.tf`)

- Engine: `postgres` 15
- Instance class: `db.t4g.micro` (sufficient for this workload; can scale up)
- Storage: 20 GB gp3
- Multi-AZ: No (ephemeral stack)
- `skip_final_snapshot = true` (so `terraform destroy` works cleanly)
- Initial DB: `airflow` (RDS creates this). The `sec_data` DB will be created by init SQL.
- Security group: Allow inbound 5432 from ECS tasks SG only

**Init script approach:** We'll run `init-sec-db.sql` as a one-shot ECS task (like `airflow-init`) that connects to RDS and creates the `sec_data` database + runs migrations.

### B4. ElastiCache Redis (`elasticache.tf`)

- Engine: Redis 7
- Node type: `cache.t4g.micro`
- Single node (no cluster mode)
- Security group: Allow inbound 6379 from ECS tasks SG

### B5. EFS (`efs.tf`)

- Encrypted at rest
- Performance mode: generalPurpose
- Mount targets in both private subnets
- Access point for `/opt/airflow/data` (UID 50000, matching Airflow container user)
- Security group: Allow inbound 2049 from ECS tasks SG

### B6. ECR (`ecr.tf`)

- Single repository: `sec-scraper-airflow`
- Lifecycle policy: Keep last 5 images

### B7. Secrets Manager (`secrets.tf`)

Store these as a JSON secret:
```json
{
  "POSTGRES_HOST": "<rds-endpoint>",
  "POSTGRES_PORT": "5432",
  "POSTGRES_DB": "sec_data",
  "POSTGRES_USER": "airflow",
  "POSTGRES_PASSWORD": "<generated>",
  "SEC_USER_AGENT": "drclive SEC scraper (you@drclive.net)"
}
```

Also generate `postgres.yaml` content and write it to EFS via init task, or inject individual env vars and modify the code to accept env-var-based Postgres config (preferred — see A6 below).

### B8. ECS Cluster & Task Definitions (`ecs.tf`)

**Cluster:** Fargate-only, no EC2 capacity providers.

**Task definitions** (one per compose service):

| Task Definition | CPU | Memory | Essential | Notes |
|----------------|-----|--------|-----------|-------|
| `airflow-init` | 256 | 512 | one-shot | Runs `airflow db migrate`, then the init-sec-db SQL, then deploy_migrations.py |
| `airflow-api-server` | 512 | 1024 | long-running | Behind ALB, healthcheck on /api/v2/monitor/health |
| `airflow-scheduler` | 256 | 512 | long-running | |
| `airflow-dag-processor` | 256 | 512 | long-running | |
| `airflow-worker` | 1024 | 2048 | long-running | Needs memory for JSON processing |
| `airflow-triggerer` | 256 | 512 | long-running | |

All tasks:
- Use the ECR image
- Mount EFS volume for `/opt/airflow/data` and `/opt/airflow/logs`
- Get secrets from Secrets Manager (injected as env vars)
- Set the same `AIRFLOW__*` environment variables as compose.yaml (pointing to RDS and ElastiCache endpoints)
- Execution role: pull from ECR, read secrets, write CloudWatch logs
- Task role: EFS access

### B9. ALB (`alb.tf`)

- Internet-facing ALB in public subnets
- Listener: HTTP :8080 → target group
- Target group: `airflow-api-server` ECS service
- Health check: `/api/v2/monitor/health`
- Security group: Restrict inbound to your IP (variable `allowed_cidr`)

### B10. IAM Roles (`iam.tf`)

- **ECS Task Execution Role:** ECR pull, Secrets Manager read, CloudWatch Logs
- **ECS Task Role:** EFS read/write (via access point), outbound internet (SEC API, Yahoo Finance)

---

## Track A (continued): Code changes for AWS compatibility

### A6. Support env-var-based Postgres config (eliminate `postgres.yaml` dependency)

Currently the code reads `postgres.yaml` for DB connection info. On AWS with Secrets Manager, it's simpler to inject env vars directly. Add fallback logic to `get_postgres_config()`:

**File:** `plugins/scripts/sec_scraper/postgres/helpers.py`

```python
def get_postgres_config(config_path: str) -> Dict[str, Any]:
    # Try env vars first (AWS Secrets Manager injects these)
    host = os.environ.get("POSTGRES_HOST", "").strip()
    if host:
        return {
            "host": host,
            "port": int(os.environ.get("POSTGRES_PORT", "5432")),
            "database": os.environ.get("POSTGRES_DB", "sec_data"),
            "user": os.environ.get("POSTGRES_USER", "airflow"),
            "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
            "schema": os.environ.get("POSTGRES_SCHEMA", "sec_raw"),
        }
    # Fall back to YAML file (local Docker setup)
    ...existing code...
```

This means both Docker Compose (with `postgres.yaml`) and AWS (with env vars from Secrets Manager) work without changes to the Dockerfile.

### A7. Handle `init-sec-db.sql` for RDS

RDS doesn't support `docker-entrypoint-initdb.d`. We need a Python init step that:
1. Connects to the `airflow` database
2. Creates `sec_data` if it doesn't exist
3. Runs `deploy_migrations.py`

**File:** `plugins/scripts/sec_scraper/postgres/init_databases.py`

Small script that wraps the existing `ensure_database_exists()` from `deploy_migrations.py` and then runs migrations. This gets called by the ECS init task.

---

## Deployment & Operations Scripts

### D1. Build and push Docker image

**File:** `scripts/aws/build_and_push.sh`

```bash
#!/bin/bash
# Build Docker image, tag for ECR, and push
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=${AWS_REGION:-us-east-1}
REPO="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/sec-scraper-airflow"

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REPO
docker build -t sec-scraper-airflow .
docker tag sec-scraper-airflow:latest $REPO:latest
docker push $REPO:latest
```

### D2. Deploy (spin up)

**File:** `scripts/aws/deploy.sh`

```bash
#!/bin/bash
set -e
cd infra
terraform init
terraform apply -auto-approve

# Wait for RDS to be available
echo "Waiting for RDS..."
aws rds wait db-instance-available --db-instance-identifier sec-scraper-db

# Run init task (db migrate + create sec_data DB + migrations)
echo "Running init task..."
# (ECS run-task for airflow-init)

# Start all services
echo "Starting Airflow services..."
# (ECS update-service desired-count=1 for each service)

echo "Airflow API available at: $(terraform output -raw alb_url)"
```

### D3. Trigger DAG run and monitor

**File:** `scripts/aws/trigger_and_monitor.sh`

```bash
#!/bin/bash
ALB_URL=$(cd infra && terraform output -raw alb_url)

# Trigger DAG run via Airflow REST API
curl -X POST "${ALB_URL}/api/v2/dags/sec_scraper/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"conf": {}}'

# Poll for completion
while true; do
  STATE=$(curl -s "${ALB_URL}/api/v2/dags/sec_scraper/dagRuns" | jq -r '.dag_runs[0].state')
  echo "DAG state: $STATE"
  if [ "$STATE" = "success" ] || [ "$STATE" = "failed" ]; then
    break
  fi
  sleep 30
done
```

### D4. Verify data

**File:** `scripts/aws/verify_data.sh`

Connects to RDS (via bastion or direct if public) and runs validation queries:
- Row counts in all tables
- Latest ingest_date
- Latest price_date
- Sample valuation scores

### D5. Teardown (spin down)

**File:** `scripts/aws/teardown.sh`

```bash
#!/bin/bash
cd infra
terraform destroy -auto-approve
```

Since we use `skip_final_snapshot`, RDS is deleted cleanly. EFS data is also destroyed. If you want to preserve data between runs, we can snapshot RDS or use S3 export — but for ephemeral "spin up, analyze, spin down" this is clean.

---

## Implementation Order

1. **A1** — Switch DAG to `schedule=None` (5 min)
2. **A6** — Env-var Postgres config fallback (15 min)
3. **A7** — Init databases script for RDS (15 min)
4. **A4** — `max_ciks=0` means "all" (5 min)
5. **A2** — Price backfill logic (30 min)
6. **A5** — Pipeline run log table (15 min)
7. **B1–B10** — Terraform infra (the bulk: ~2–3 hours)
8. **D1–D5** — Deploy/operate/teardown scripts (30 min)
9. **Tests** — Update/add tests for new code (30 min)

---

## Variables / Decisions Needed From You

1. **AWS Region** — Default `us-east-1`?
2. **Your IP** — For restricting ALB access (security group). Can also use a VPN CIDR.
3. **SEC_USER_AGENT** — Keep `"drclive SEC scraper (you@drclive.net)"`?
4. **RDS instance size** — `db.t4g.micro` is cheapest (~$12/mo), `db.t4g.small` for more headroom (~$25/mo). Since this is ephemeral it barely matters.
5. **Preserve data between runs?** — If yes, we'd snapshot RDS before teardown and restore on next deploy. If no, clean destroy is simpler.
6. **Airflow auth** — The current setup uses `simple_auth_manager_passwords.json`. For AWS we can keep that (embed in Secrets Manager) or skip auth for the short-lived stack (restrict via security group only).
7. **S3 for raw data** — Currently all data is local disk (EFS). Want to also push to S3 for durability, or is EFS-only fine for ephemeral runs?
