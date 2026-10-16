# Securities Trade Operations Analytics Platform

An enterprise data platform built on **Microsoft Fabric** using a **Medallion Lakehouse architecture** (Bronze, Silver, Gold). The solution ingests raw trading and settlement feeds, enforces strict schema validation and business rule integrity, isolates non-compliant records into exception tables, and prepares dimensionally modeled reporting tables for trade reconciliation and exposure analysis.

---

## 🏛️ Architecture Overview

The platform implements the standard Medallion architecture within a unified Fabric Lakehouse:

[Raw Feeds: Trades & Securities]
│
▼
┌──────────────────┐
│   BRONZE LAYER   │  Raw Delta ingestion preserving source format
│  (Delta Lake)    │  (fact_trade_raw, dim_security_raw)
└─────────┬────────┘
│  PySpark Transformation & Schema Validation
▼
┌──────────────────┐
│   SILVER LAYER   │  Validated transactions & cleaned dimensional data
│  (Delta Lake)    │  (fact_trade_silver, dim_security_silver)
└─────────┬────────┘
│           ┌────────────────────────────────┐
├──────────►│ EXCEPTION / AUDIT LOGS         │ Non-compliant trades
│           │ (fact_trade_exceptions)       │ (price <= 0, invalid IDs)
│           └────────────────────────────────┘
▼  Dimensional Modeling & Star Schema
┌──────────────────┐
│    GOLD LAYER    │  Curated Star Schema optimized for analytics
│  (Delta Lake)    │  (fact_daily_trade_summary, dim_securities)
└─────────┬────────┘
│
▼
┌──────────────────┐
│ SERVING & BI     │  DirectLake / SQL Analytics Endpoint
│ (Semantic Model) │  Sub-second reconciliation & exposure dashboards
└──────────────────┘



---

## 🛠️ Key Capabilities & Technical Implementation

* **Medallion Lakehouse Storage**: Data persisted in Delta Lake format backed by Microsoft OneLake, leveraging Liquid Clustering on high-cardinality keys (`trade_id`, `security_id`).
* **PySpark Transformation Engine**: Fabric Notebooks (`02_silver_transformation_and_validation`) handle type casting, timestamp normalization, and multi-condition trade logic.
* **Data Quality & Exception Isolation**:
  * Rules enforce positive volume and price thresholds (`quantity > 0`, `price > 0`).
  * Cross-table referential integrity verifies traded instruments against security master definitions.
  * Invalid trades are quarantined into `fact_trade_exceptions` with detailed `rejection_reason` codes for compliance review.
* **Orchestration & Audit Pipelines**:
  * Fabric Data Factory pipeline (`pl_securities_daily_etl`) coordinates notebook execution dependencies.
  * Integrated **Lookup activities** execute automated record-count assertions post-run to guarantee zero data loss between ingestion and final gold aggregations.
* **Version Control & CI/CD**:
  * Workspaces directly synced to Git via native Microsoft Fabric Git Integration.
  * Notebooks and pipelines fully serialized into modular `.py` and `.json` definitions for branch review and release management.

---

## 📂 Repository Artifacts

* `02_silver_transformation_and_validation.Notebook/`: PySpark validation and transformation logic.
* `pl_securities_daily_etl.DataPipeline/`: Fabric Data Factory pipeline configuration and activity graphs.
* `SecuritiesLakehouse.Lakehouse/`: Metadata definitions for the OneLake storage endpoints.
* `SecuritiesModel.SemanticModel/`: Star schema data model definitions for DirectLake analytics.

* 
