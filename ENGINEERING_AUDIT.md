# Engineering Audit — Shipment Analytics Pipeline

**Auditor:** Abdullah Al Junaibi  
**Date:** March 10, 2026  
**Scope:** Full pipeline audit — infrastructure, extraction, transformation, loading, and orchestration

---

## Executive Summary

The pipeline contained **13 distinct issues** spanning security vulnerabilities, data quality bugs, infrastructure conflicts, and missing resilience patterns. All issues have been identified, categorized, and resolved. The fixed pipeline passes 17 automated tests covering extraction validation, transformation correctness, analytics integrity, and idempotency.

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

#### 6. Duplicate Customer Tier (SCD Not Handled)
- **Severity:** High
- **Category:** Data Quality
- **Detail:** `CUST002` appears twice in `customer_tiers.csv` — Platinum (Jan 1) and Gold (Feb 15). No Slowly Changing Dimension handling existed.
- **Impact:** Non-deterministic tier assignment depending on insertion order.
- **Fix:** SCD Type 1 — keep the most recent `tier_updated_date` per customer. CUST002 correctly resolves to Gold.

#### 7. INNER JOIN Drops Orphan Customers
- **Severity:** High
- **Category:** Data Quality
- **Original code:** `transform_data.py` used `INNER JOIN`, which silently dropped `CUST999` (a customer with shipments but no tier entry).
- **Impact:** Lost revenue data — shipments for unknown customers disappear from analytics.
- **Fix:** Changed to `LEFT JOIN` with `COALESCE(tier, 'Unknown')`. CUST999 now correctly appears as "Unknown" tier.

### 🟡 Medium (4)

#### 8. No API Retry Logic
- **Severity:** Medium
- **Category:** Resilience
- **Detail:** The API returns HTTP 500 intermittently (deliberate flakiness on every ~7th request). Original code had no retry mechanism.
- **Impact:** Pipeline fails non-deterministically on any run that hits the flaky request.
- **Fix:** Added 3-attempt retry with 5-second delay between attempts. Raises `RuntimeError` only after all attempts exhausted.

#### 9. Non-Idempotent Analytics Load
- **Severity:** Medium
- **Category:** Data Integrity
- **Detail:** `load_analytics.py` used `INSERT` without clearing existing data. Re-running the pipeline duplicated all analytics rows.
- **Impact:** Analytics totals double on every re-run.
- **Fix:** Added `TRUNCATE TABLE analytics.shipping_spend_by_tier` before `INSERT`. Verified idempotency via automated test.

#### 10. Non-Atomic Table Swaps
- **Severity:** Medium
- **Category:** Reliability
- **Detail:** All scripts used `DROP TABLE IF EXISTS; CREATE TABLE` — if the process crashes between DROP and data load, the table is empty.
- **Fix:** Write to `_new` table, then `DROP` old + `ALTER TABLE ... RENAME`. The old table exists until the new one is fully loaded.

#### 11. Hardcoded Database Credentials
- **Severity:** Medium
- **Category:** Security
- **Detail:** All scripts hardcoded `host="postgres"`, `database="airflow"`, etc. directly in source.
- **Fix:** All credentials now read from environment variables with sensible defaults: `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.

### 🔵 Low (2)

#### 12. PostgreSQL Port Conflict with Host
- **Severity:** Low
- **Category:** Infrastructure
- **Detail:** `docker-compose.yml` mapped postgres to host port `5432`, which conflicts with any existing PostgreSQL instance on the host.
- **Fix:** Remapped to `5433:5432`.

#### 13. API Artificial Latency (Every 7th Request)
- **Severity:** Low (by design)
- **Category:** Resilience testing
- **Detail:** The mock API injects a 5-second sleep on every 7th request. This is a deliberate challenge feature, not a bug to fix in the API — but the pipeline must handle it.
- **Impact:** Without retry logic (#8) or timeouts, the pipeline hangs or fails intermittently.
- **Fix:** Handled via retry logic in extraction and request timeouts (`timeout=30`).

---

## Test Coverage

| Category | Tests | Status |
|----------|-------|--------|
| Extraction | 8 tests (count, dedup, neg cost, null ID, cancelled, tier SCD) | ✅ All pass |
| Transformation | 3 tests (row count, orphan mapping, tier coverage) | ✅ All pass |
| Analytics | 5 tests (data exists, row count, no negatives, spend match, no dupes) | ✅ All pass |
| Idempotency | 1 test (TRUNCATE + INSERT stability) | ✅ All pass |
| **Total** | **17 tests** | **✅ 17/17 pass (0.25s)** |

---

## Summary of Changes

| File | Changes |
|------|---------|
| `docker-compose.yml` | Fixed YAML indent, port conflicts (API→8000, PG→5433), added tests volume |
| `scripts/extract_shipments.py` | Parameterized SQL, retry logic, validation, dedup, atomic swap, env vars |
| `scripts/extract_customer_tiers.py` | SCD handling, atomic swap, env vars |
| `scripts/transform_data.py` | LEFT JOIN + COALESCE, atomic swap, source validation, env vars |
| `scripts/load_analytics.py` | Idempotent TRUNCATE+INSERT, env vars, analytics summary output |
| `tests/test_pipeline.py` | 17 automated tests across 4 categories |
| `tests/conftest.py` | Test configuration and DB connection helper |
