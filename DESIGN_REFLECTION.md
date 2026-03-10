# Design Reflection — Shipment Analytics Pipeline

**Author:** Abdullah Al Junaibi  
**Date:** March 10, 2026

---

## 1. The Most Critical Issue

**SQL injection in `extract_shipments.py`** was the most critical issue. While the other bugs caused data quality problems or deployment failures, SQL injection is a security vulnerability that could lead to complete database compromise — data exfiltration, table deletion, or privilege escalation.

What made it particularly dangerous: the `extract_customer_tiers.py` script in the same codebase used parameterized queries correctly, creating a false sense of security. A code reviewer might assume "they know how to do parameterized queries" and skip reviewing the other script.

The fix was straightforward — replace f-string interpolation with `%s` placeholders — but the lesson is systemic: **security patterns must be enforced at the architecture level** (linters, pre-commit hooks, code review checklists), not left to individual developer discipline.

---

## 2. Trade-offs in My Fixes

### Deduplication Strategy: Last-Wins
I chose to keep the **last occurrence** of duplicate `shipment_id` entries. This assumes the API returns data in chronological order and later entries are corrections. An alternative would be to keep the first occurrence, or to flag duplicates for manual review. I chose last-wins because it's the most common pattern in event-driven systems and the API provides no explicit version field.

### Orphan Customer Handling: Map to 'Unknown'
When a shipment references a `customer_id` not in the tiers CSV (e.g., `CUST999`), I mapped the tier to `'Unknown'` via `LEFT JOIN` + `COALESCE`. The alternative was to drop these shipments entirely (`INNER JOIN`, as the original code did). I kept them because **losing revenue data is worse than having an incomplete tier classification**. The `'Unknown'` tier is visible in analytics and can trigger a data quality investigation.

### Atomic Table Swaps vs. UPSERT
I used the `CREATE _new → DROP old → RENAME` pattern instead of `UPSERT` (`INSERT ... ON CONFLICT`). Trade-off: atomic swaps are simpler and guarantee a clean state, but they briefly lock the table during rename. For a batch pipeline that runs on a schedule (not real-time), this is acceptable. UPSERT would be better for streaming or high-frequency updates.

### Retry Logic: Fixed Delay vs. Exponential Backoff
I used a fixed 5-second delay between retries (3 attempts). Exponential backoff would be more robust for production systems with rate limiting. I chose fixed delay because the API's failure pattern is deterministic (every ~7th request) and 5 seconds is sufficient for recovery.

---

## 3. Scaling to 100x Data Volume

At 100x scale (~2,100 shipments per batch, or continuous streaming), the current architecture would need these changes:

### Short-term (10x–50x)
- **Batch INSERT instead of row-by-row:** Replace the Python loop with `executemany()` or `COPY FROM` for bulk loading. Current row-by-row inserts become the bottleneck first.
- **Connection pooling:** Use `psycopg2.pool` or PgBouncer instead of opening/closing connections per script.
- **Pagination:** The API currently returns all shipments in one response. At scale, paginate with `?page=1&limit=1000`.

### Medium-term (50x–100x)
- **Partitioned tables:** Partition `staging.shipments` and `analytics.shipping_spend_by_tier` by `year_month`. This makes both writes and reads faster for time-range queries.
- **Incremental extraction:** Instead of full-table replace on every run, track a watermark (`last_extracted_at`) and only fetch new/modified shipments. This reduces API load and processing time from O(n) to O(delta).
- **Parallel extraction:** Run `extract_shipments` and `extract_customer_tiers` in parallel (they're independent). Airflow already supports this via task dependencies — just remove the serial chain.

### Long-term (100x+)
- **Move to a columnar store:** For analytics queries (aggregation by tier/month), a columnar database like ClickHouse or DuckDB would outperform PostgreSQL row-store by 10–100x.
- **Stream processing:** Replace batch extraction with a Kafka/event-driven pipeline. Shipments arrive as events, are validated and enriched in real-time, and analytics are maintained as materialized views.
- **Data quality framework:** Replace inline validation with a dedicated tool like Great Expectations or dbt tests. This separates data quality rules from pipeline logic and makes them auditable.

---

## 4. Code Walkthrough — Key Design Decisions

### Extract (`extract_shipments.py`)
```
API → Retry (3x) → Validate → Deduplicate → Atomic Load
```
The extraction script is the most complex because it handles the most failure modes: network errors (retry), bad data (validation), duplicate records (dedup), and crash safety (atomic swap). Each concern is a separate step, making it easy to test and modify independently.

### Extract (`extract_customer_tiers.py`)
```
CSV → Parse → SCD Dedup (latest date) → Atomic Load
```
Simpler than shipments because CSV is a local file (no network failures). The key decision was SCD Type 1 handling: when a customer appears twice, keep the row with the most recent `tier_updated_date`.

### Transform (`transform_data.py`)
```
Validate Sources → LEFT JOIN + COALESCE → Atomic Load
```
The transform is a single SQL operation. Using `LEFT JOIN` instead of `INNER JOIN` was the critical fix — it preserves all shipment data even when tier information is missing.

### Load (`load_analytics.py`)
```
Validate Source → TRUNCATE → INSERT (GROUP BY tier, month) → Summary
```
The `TRUNCATE` before `INSERT` pattern makes this step idempotent. Running it twice produces the same result. The summary output serves as a human-readable verification step.

### Orchestration (`docker-compose.yml`)
```
postgres → api → airflow-webserver → airflow-scheduler
```
Services start in dependency order. The three infrastructure bugs (YAML indent, port conflicts) were all in this file — the "glue" layer is often where the most subtle bugs hide because it's the least tested.

---

## 5. What I Would Do Differently

1. **Add data quality assertions inside the pipeline** — not just in tests. For example, after extraction, assert that shipment count is within an expected range. If it drops 50% from the previous run, halt and alert.

2. **Add logging instead of print statements.** Python's `logging` module with structured output (JSON) would make it easier to search and alert on pipeline events in production.

3. **Add a `requirements.txt` for the scripts directory** with pinned versions of `requests` and `psycopg2`. The current setup relies on whatever is installed in the Airflow Docker image.

4. **Add a Makefile or shell script** for common operations: `make run-pipeline`, `make test`, `make reset-db`. This reduces the cognitive load for new developers.
