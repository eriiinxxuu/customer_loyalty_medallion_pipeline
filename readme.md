# Customer Loyalty Medallion Pipeline

![Python](https://img.shields.io/badge/Python-3.12+-3776AB.svg)
![Databricks](https://img.shields.io/badge/Databricks-Serverless_Photon-FF3621.svg)
![PySpark](https://img.shields.io/badge/PySpark-Structured_Processing-E25A1C.svg)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID_Transactions-00ADD8.svg)
![Terraform](https://img.shields.io/badge/Terraform-Infrastructure_as_Code-623CE4.svg)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Data_Governance-1D6FA5.svg)
![Data Volume](https://img.shields.io/badge/Data_Volume-50M_Rows-green.svg)



## Introduction
A large-scale data processing platform built on Databricks for a customer loyalty programme, ingesting and transforming transaction, reward, and member activity records across a medallion architecture (Bronze → Silver → Gold).

## Architecture

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
├── databricks/
    ├── 01_generate_data.py
│   ├── 02_bronze.py
│   ├── 03_silver.py
│   ├── 04_data_quality.py
│   ├── 05_gold.py
│   └── 06_sql_analytics.py
└── terraform/
    ├── main.tf
    ├── variables.tf
    ├── catalog.tf
    └── cluster_policy.tf
```

## Pipeline
 
Orchestrated end-to-end via Databricks Workflows with explicit task dependencies:
 
```
data_generate → bronze_ingest → silver_transform → data_quality_checks → gold_aggregate
```
## Performance Results
 
Spark performance optimised using adaptive query execution, dynamic partition pruning, ordering, and broadcast joins:
 
| Optimisation | Implementation | Result |
|---|---|---|
| Adaptive query execution | Enforced by Databricks Photon runtime | Shuffle partitions dynamically coalesced at runtime based on actual data size |
| Dynamic partition pruning | Date filter pushed down to scan time on partitioned Bronze table | Only relevant partitions scanned, reducing I/O significantly |
| Broadcast join | Store dimension (~500 rows) broadcast to all executors | 38.6% reduction in join duration, eliminates shuffle |
| Ordering | `ORDER BY member_id` applied before Gold write | Related rows co-located in output files, faster downstream member lookups |
| Z-ORDER (Silver) | `ZORDER BY (member_id, transaction_date)` post-write | File compaction, improved file skipping on filtered reads |
| Z-ORDER (Gold) | `ZORDER BY (member_id)` post-write | File compaction on member lookups |
| Deduplication | Window function on `transaction_id` ordered by `ingested_at` | 50M Bronze rows → 5M Silver rows (10x reduction) |
 
 
 
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
