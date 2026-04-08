# Customer Loyalty Medallion Pipeline

![Python](https://img.shields.io/badge/Python-3.12+-3776AB.svg)
![Databricks](https://img.shields.io/badge/Databricks-Serverless_Photon-FF3621.svg)
![PySpark](https://img.shields.io/badge/PySpark-Structured_Processing-E25A1C.svg)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID_Transactions-00ADD8.svg)
![Terraform](https://img.shields.io/badge/Terraform-Infrastructure_as_Code-623CE4.svg)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Data_Governance-1D6FA5.svg)
![Data Volume](https://img.shields.io/badge/Data_Volume-50M_Rows-green.svg)



## Introduction
A production-grade data platform built on Databricks Medallion Architecture, processing 50M+ synthetic retail loyalty transactions through Bronze → Silver → Gold layers with automated schema evolution, incremental loading, and SCD Type 2 history tracking.

## Architecture
 
```
dbldatagen (simulate upstream system)
         │ daily append (50K rows/day)
         ▼
Landing Volume  (/Volumes/loyalty/landing/raw_data/transactions)
         │ Delta CDF + version tracking
         ▼
Bronze Delta  (loyalty.bronze.transactions)
  append-only · ingested_at watermark
         │ ingested_at watermark
         ▼
Silver Delta  (loyalty.silver.transactions)
  DQ · dedup · schema reconciliation · MERGE
         │ Delta CDF + version tracking
         ▼
Gold Delta  (loyalty.gold.member_summary)        member_tier_history (SCD Type 2)
  member aggregations · SCD Type 1               full tier change history
         │
         ▼
Databricks SQL Warehouse
  churn propensity · LTV · reward breakage · segment engagement
```

## Techinical Skills
| Layer | Technology |
|---|---|
| Processing | Databricks (Serverless + Photon), PySpark, SQL |
| Storage | Delta Lake, Unity Catalog |
| Orchestration | Databricks Workflows |
| Infrastructure | Terraform (Databricks provider) |
| Data generation | dbldatagen |

## Infrastructure

```
customer_loyalty_medallion_pipeline/
├── README.md
├── .gitignore
├── analytics/
|   ├── member_analytics.sql
├── utilis/
|   ├── 00_generate_historical.py
|   ├── generate_store_dim.py
├── test/
|   ├── execution_plan_evidence.py
|   ├── schema_drift_test.py
├── databricks/
|   ├── 01_generate_data.py
│   ├── 02_bronze.py
│   ├── 03_silver.py
│   ├── 04_data_quality.py
│   ├── 05_gold.py
│   └── 06_scd2_tier_history.py
└── terraform/
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── catalog.tf
    └── cluster_policy.tf
```

## Pipeline
 
**Databricks Workflow** — 5 tasks, daily 05:00 AEST:
```
bronze_layer → silver_layer → dq_silver_to_gold → gold_layer → scd2_tier_history

```

## Incremental Load Strategy
 
Three layers, same strategy — Delta CDF + version tracking via a shared `VERSION_TABLE`:
 
| Layer | Source | Version Tracks |
|---|---|---|
| Landing → Bronze | Delta CDF | Landing Delta version |
| Bronze → Silver | Delta CDF | Bronze Delta version |
| Silver → Gold | Delta CDF | Silver Delta version |
 
All three jobs use the same `version_utils.py` helpers (`get_last_version`, `save_version`, `read_cdf`). Version is committed only after a successful write — if a job fails mid-run, the version is not advanced and the next run reprocesses the same batch cleanly.
 
```
loyalty.meta.job_versions
 
job_name               | last_processed_version
bronze_ingestion       | 4    ← tracks Landing Delta version
silver_transactions    | 3    ← tracks Bronze Delta version
gold_member_summary    | 2    ← tracks Silver Delta version
```

## SCD Implementation
 
| Table | Type | Behaviour |
|---|---|---|
| `gold.member_summary` | SCD Type 1 | Current tier only, overwritten on change |
| `gold.member_tier_history` | SCD Type 2 | Full history, one row per tier period |
 
**Query member tier at any point in time:**
```sql
SELECT member_id, current_tier
FROM loyalty.gold.member_tier_history
WHERE effective_date <= '2023-03-01'
  AND expiry_date    >  '2023-03-01'
```

## Schema Evolution
 
`reconcile_schema()` handles three upstream drift scenarios automatically, no manual intervention:
 
| Scenario | Handling |
|---|---|
| New column added upstream | `ALTER TABLE ADD COLUMN` before MERGE, existing rows filled with null |
| Column deleted upstream | Silver schema preserved, missing column filled with null |
| Type change upstream | `try_cast` pre-detects lossy conversions; castable rows proceed, failed rows → quarantine |
 
## Data Quality
 
- **Bronze:** raw data preserved as-is, no DQ — ensures original records always recoverable
- **Silver entry:** `amount > 0`, `member_id` not null, `transaction_date` not null — failures → `transactions_quarantine` table, job continues
- **Pre-Gold:** Great Expectations validation (GE v1.15.2) — critical failures block Gold computation
 
**Quarantine pattern:** 2,000 bad rows out of 50M → quarantine, not job failure. Bad records stored with `quarantine_reason` and `quarantine_ts` for inspection and reinjection.
 
 ## Performance Optimizations
 
| Optimisation | Implementation | Result |
|---|---|---|
| Photon vectorized engine | All queries run on Databricks Photon runtime | Full vectorized execution across scan, join, window, and aggregation — confirmed by `The query is fully supported by Photon` in all execution plans |
| Adaptive Query Execution | Enforced by Databricks Serverless runtime, `shuffle.partitions=200` | Shuffle partitions dynamically coalesced at runtime based on actual data size |
| Dynamic Partition Pruning | Silver partitioned by `transaction_month`; date filters pushed to file scan level | Photon `DictionaryFilters` applied at scan time — 100% of out-of-range partition files skipped ([execution plan](https://github.com/eriiinxxuu/customer_loyalty_medallion_pipeline/blob/master/screenshots/evidence2.png)) |
| Broadcast Join | Store dimension (~500 rows) broadcast to all executors via `EXECUTOR_BROADCAST` | Silver-side 50M rows require zero shuffle — verified via `PhotonBroadcastHashJoin` in physical plan ([execution plan](https://github.com/eriiinxxuu/customer_loyalty_medallion_pipeline/blob/master/screenshots/evidence1.1.png)) |
| PhotonTopK Deduplication | `row_number()` window function on `transaction_id` ordered by `ingested_at DESC` | Photon optimizes to `PhotonTopK` — single pass per partition, no groupBy + join shuffle ([execution plan](https://github.com/eriiinxxuu/customer_loyalty_medallion_pipeline/blob/master/screenshots/evidence4.png)) |
| Z-ORDER (Gold) | `OPTIMIZE loyalty.gold.member_summary ZORDER BY (member_id)` post-write | File compaction and data co-location for downstream member lookups and SQL analytics queries |
| Incremental Processing | Delta CDF version tracking (Silver → Gold) and `ingested_at` watermark (Bronze → Silver) | Daily pipeline processes 50K rows instead of re-scanning 50M — verified via `DESCRIBE HISTORY` ([evidence](https://github.com/eriiinxxuu/customer_loyalty_medallion_pipeline/blob/master/screenshots/evidence3.png)) |
 
## Infrastructure (Terraform)
 
Databricks workspace resources managed as code via the official Databricks Terraform provider:
 
- Unity Catalog schemas (`bronze`, `silver`, `gold`, `landing`)
- Landing Volume (`raw_data`)
- Job cluster policy (autoscaling 2–8 workers, 20-min autotermination)
 
```bash
cd terraform
terraform init
terraform plan
terraform apply
```
