# Engineering Audit — Shipment Analytics Pipeline

**Auditor:** Abdullah Al Junaibi  
**Date:** March 15, 2026  
**Scope:** Full pipeline audit — infrastructure, extraction, transformation, loading, and orchestration

---

## Executive Summary

The pipeline contained **14 distinct issues** spanning security vulnerabilities, data quality bugs, infrastructure conflicts, and missing resilience patterns. All issues have been identified, categorized, and resolved. Production hardening since the initial audit added a shared utility layer in `scripts/common/`, structured run observability in `ops.pipeline_runs` and `ops.stage_runs`, append-only raw/rejection ledgers keyed by `pipeline_run_id`, atomic table swaps across staging and analytics loads, and environment-based credential management via `.env.example` and Docker Compose substitution. The fixed pipeline now passes 35 automated tests covering extraction validation, raw ingest auditability, transformation correctness, analytics integrity, idempotency, and hardening regressions.

---

## Issues Found

### 🔴 Critical (3)

#### 1. SQL Injection in `extract_shipments.py`
- **Severity:** Critical
- **Category:** Security
- **Original code:**
  ```python
  cursor.execute(f"INSERT ... VALUES ('{shipment['shipment_id']}', ...)")
  ```
- **Risk:** Any API response containing SQL metacharacters (e.g., `'; DROP TABLE --`) could execute arbitrary SQL. The customer_tiers script used parameterized queries correctly, making this inconsistency a clear oversight.
- **Fix:** Replaced all f-string interpolation with parameterized queries (`%s` placeholders).

#### 2. YAML Indentation Error in `docker-compose.yml`
- **Severity:** Critical
- **Category:** Infrastructure
- **Original code:**
  ```yaml
  services:
    # PostgreSQL Database
   postgres:  # ← 1-space indent instead of 2
  ```
- **Impact:** `docker compose up` fails immediately — the entire pipeline cannot start.
- **Fix:** Corrected `postgres:` to use standard 2-space indentation.

#### 3. Port Conflict — API and Airflow Both on 8080
- **Severity:** Critical
- **Category:** Infrastructure
- **Original config:** Both `api` and `airflow-webserver` exposed port `8080` on the host.
- **Impact:** One service fails to bind, preventing either the API or the Airflow UI from being accessible.
- **Fix:** Remapped API from `8080:8000` to `8000:8000`. Airflow webserver keeps `8080:8080`.

### 🟠 High (4)

#### 4. Duplicate Shipment Data (SHP002)
- **Severity:** High
- **Category:** Data Quality
- **Detail:** `SHP002` appears twice in the API response with different costs ($45.00 and $47.00). No deduplication existed.
- **Impact:** Double-counted revenue in analytics, inflated shipment counts.
- **Fix:** Deduplication by `shipment_id` — keep last occurrence (most recent data wins).

#### 5. No Data Validation on Extraction
- **Severity:** High
- **Category:** Data Quality
- **Details:** Three problematic records passed through unfiltered:
  - `SHP012`: Negative shipping cost (`-$5.00`)
  - `SHP014`: Null `customer_id`
  - `SHP017`: Cancelled shipment (should be excluded from analytics)
- **Impact:** Negative costs distort spend totals; null customer_id causes join failures; cancelled shipments inflate counts.
- **Fix:** Added validation rules: reject negative costs, null customer IDs, and cancelled shipments. Clear rejection logging for auditability.

#### 6. Customer Tier History Was Collapsed Incorrectly
- **Severity:** High
- **Category:** Data Quality
- **Detail:** `CUST002` appears twice in `customer_tiers.csv` — Platinum (Jan 1) and Gold (Feb 15). No Slowly Changing Dimension handling existed.
- **Impact:** Historical shipments would be assigned the wrong tier if a customer changed status mid-quarter.
- **Fix:** Preserved effective-dated customer tier history in `staging.customer_tiers` and joined shipments to the most recent tier record on or before each shipment date. `CUST002` now resolves to Platinum before 2024-02-15 and Gold afterward.

#### 7. INNER JOIN Drops Orphan Customers
- **Severity:** High
- **Category:** Data Quality
- **Original code:** `transform_data.py` used `INNER JOIN`, which silently dropped `CUST999` (a customer with shipments but no tier entry).
- **Impact:** Lost revenue data — shipments for unknown customers disappear from analytics.
- **Fix:** Changed to `LEFT JOIN` with `COALESCE(tier, 'Unknown')`. CUST999 now correctly appears as "Unknown" tier.

### 🟡 Medium (5)

#### 8. No Raw Ingest Audit Trail for Source Data
- **Severity:** Medium
- **Category:** Observability
- **Detail:** Original extraction loaded directly into staging, which made it difficult to inspect source payloads, compare accepted vs. rejected records, or explain row-count changes after validation.
- **Impact:** Reduced traceability during debugging and weak evidence for why specific records were excluded from downstream analytics.
- **Fix:** Added raw ingest ledgers: `raw.shipments_raw`, `raw.customer_tiers_raw`, `raw.shipment_rejections`, and `raw.customer_tier_rejections`. This preserves the original source rows and deterministic rejection metadata.

#### 9. No API Retry Logic
- **Severity:** Medium
- **Category:** Resilience
- **Detail:** The API returns HTTP 500 intermittently (deliberate flakiness on every ~7th request). Original code had no retry mechanism.
- **Impact:** Pipeline fails non-deterministically on any run that hits the flaky request.
- **Fix:** Added 3-attempt retry with 5-second delay between attempts. Raises `RuntimeError` only after all attempts exhausted.

#### 10. Non-Idempotent Analytics Load
- **Severity:** Medium
- **Category:** Data Integrity
- **Detail:** `load_analytics.py` used `INSERT` without clearing existing data. Re-running the pipeline duplicated all analytics rows.
- **Impact:** Analytics totals double on every re-run.
- **Fix:** Added `TRUNCATE TABLE analytics.shipping_spend_by_tier` before `INSERT`. Verified idempotency via automated test.

#### 11. Non-Atomic Table Swaps
- **Severity:** Medium
- **Category:** Reliability
- **Detail:** All scripts used `DROP TABLE IF EXISTS; CREATE TABLE` — if the process crashes between DROP and data load, the table is empty.
- **Fix:** Write to `_new` table, then `DROP` old + `ALTER TABLE ... RENAME`. The old table exists until the new one is fully loaded.

#### 12. Hardcoded Database Credentials
- **Severity:** Medium
- **Category:** Security
- **Detail:** All scripts hardcoded `host="postgres"`, `database="airflow"`, etc. directly in source.
- **Fix:** All credentials now read from environment variables with sensible defaults: `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.

### 🔵 Low (2)

#### 13. PostgreSQL Port Conflict with Host
- **Severity:** Low
- **Category:** Infrastructure
- **Detail:** `docker-compose.yml` mapped postgres to host port `5432`, which conflicts with any existing PostgreSQL instance on the host.
- **Fix:** Remapped to `5433:5432`.

#### 14. API Artificial Latency (Every 7th Request)
- **Severity:** Low (by design)
- **Category:** Resilience testing
- **Detail:** The mock API injects a 5-second sleep on every 7th request. This is a deliberate challenge feature, not a bug to fix in the API — but the pipeline must handle it.
- **Impact:** Without retry logic (#8) or timeouts, the pipeline hangs or fails intermittently.
- **Fix:** Handled via retry logic in extraction and request timeouts (`timeout=30`).

---

## Production Hardening Additions

### Ops Observability
- **Implementation:** `scripts/common/run_context.py` now creates and writes to `ops.pipeline_runs` and `ops.stage_runs`.
- **Detail:** Every pipeline execution gets a `pipeline_run_id` generated from `uuid.uuid4()` or derived deterministically from Airflow context. Stage execution metadata is tracked per `stage_name` with `started_at`, `finished_at`, `status`, `retry_count`, `rows_read`, `rows_written`, `rows_rejected`, and `error_message`.
- **Operational value:** This replaces ad hoc log-only monitoring with queryable metadata. Operators can inspect one run end-to-end, compare retries, and reconcile row counts across `extract_shipments`, `extract_customer_tiers`, `transform_data`, and `load_analytics`.

### Append-Only Raw Layer
- **Implementation:** `scripts/extract_shipments.py` writes to `raw.shipments_raw` and `raw.shipment_rejections`; `scripts/extract_customer_tiers.py` writes to `raw.customer_tiers_raw` and `raw.customer_tier_rejections`.
- **Detail:** These raw and rejection tables are append-only and include `pipeline_run_id` on every row. Indexes `shipments_raw_pipeline_run_id_idx`, `shipment_rejections_pipeline_run_id_idx`, `customer_tiers_raw_pipeline_run_id_idx`, and `customer_tier_rejections_pipeline_run_id_idx` support run-scoped filtering.
- **Operational value:** Source history is preserved across reruns instead of being overwritten, which makes it possible to answer "what did the pipeline ingest and reject on run X?" without reconstructing state from logs.

### Atomic Table Swaps
- **Implementation:** The pipeline now uses `CREATE ..._new`, populate, `DROP` old table, then `ALTER TABLE ..._new RENAME TO ...` for `staging.customer_tiers`, `staging.shipments`, and `analytics.shipping_spend_by_tier`.
- **Detail:** This pattern keeps the current production table in place until the replacement dataset has been fully validated and loaded.
- **Operational value:** Readers either see the old complete table or the new complete table, which provides effectively zero-downtime refresh semantics for this batch pipeline. A failed load no longer leaves `customer_tiers` or analytics half-written or empty.

### Credential Externalization
- **Implementation:** `.env.example` now documents `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL`, `API_PORT`, and `PG_HOST_PORT`.
- **Detail:** `docker-compose.yml` uses `${VAR:-default}` substitution for PostgreSQL and Airflow settings, while `scripts/common/config.py` resolves runtime configuration from environment variables with local defaults.
- **Operational value:** Credentials and ports are configurable without source edits, and `.env` remains outside version control while `.env.example` documents the required interface.

---

## Test Coverage

| Category | Tests | Status |
|----------|-------|--------|
| Extraction | 16 tests (counts, dedup, validation, raw ledgers, tier history, rejection determinism) | ✅ All pass |
| Transformation | 4 tests (row count, orphan mapping, effective-dated join, tier coverage) | ✅ All pass |
| Analytics | 6 tests (data exists, row count, monthly totals, no negatives, spend match, no dupes) | ✅ All pass |
| Idempotency | 1 test (TRUNCATE + INSERT stability) | ✅ All pass |
| Hardening | 8 tests (ops audit rows, append-only raw history, index coverage, fail-fast guards, retry/error handling) | ✅ All pass |
| **Total** | **35 tests** | **✅ 35/35 pass** |

---

## Summary of Changes

| File | Changes |
|------|---------|
| `docker-compose.yml` | Fixed YAML indent, port conflicts (API→8000, PG→5433), added tests volume |
| `sql/init.sql` | Base schema creation and analytics schema contract initialization |
| `scripts/extract_shipments.py` | Parameterized SQL, retry logic, raw ingest ledger, validation, dedup, atomic swap, env vars |
| `scripts/extract_customer_tiers.py` | Raw ingest ledger, rejection ledger, effective-dated history preservation, atomic swap, env vars |
| `scripts/transform_data.py` | LEFT JOIN + COALESCE, effective-dated customer tier history join, atomic swap, source validation, env vars |
| `scripts/load_analytics.py` | Idempotent TRUNCATE+INSERT, env vars, analytics summary output |
| `scripts/common/config.py` | Shared environment-backed configuration helpers for database, API, and CSV paths |
| `scripts/common/db.py` | Shared connection and transaction helpers |
| `scripts/common/logging_utils.py` | Shared logger initialization for consistent structured stage output |
| `scripts/common/run_context.py` | Shared pipeline/stage run tracking, UUID generation, and ops schema writes |
| `.env.example` | Sample environment contract for credentials and host port overrides |
| `docker-compose.yml` | `${VAR:-default}` substitution for runtime configuration and credential injection |
| `tests/test_pipeline.py` | 27 automated functional tests across 4 categories |
| `tests/test_hardening.py` | 8 production-hardening regression tests |
| `tests/conftest.py` | Test configuration and DB connection helper |
