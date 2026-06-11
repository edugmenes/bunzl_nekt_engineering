# Bunzl — Pipedrive → BigQuery → Power BI

End-to-end data pipeline built on the [Nekt](https://nekt.com.br) platform that extracts CRM data from **Pipedrive**, transforms it using **PySpark**, and loads it into **Google BigQuery** for consumption in **Power BI**.

---

## Table of Contents

- [Context](#-context)
- [Architecture Overview](#️-architecture-overview)
  - [Bronze — Extraction](#-bronze--extraction)
  - [Silver — Transformation](#-silver--transformation)
  - [Gold — Load](#-gold--load)
- [Data Catalog](#-data-catalog)
  - [Pipedrive Bronze Tables](#pipedrive--bronze)
  - [Pipedrive Silver Tables](#pipedrive--silver)
- [Notebook: Transformation Logic](#-notebook-transformation-logic)
  - [Helper Functions](#helper-functions)
  - [Pipedrive Transformations](#pipedrive-transformations)
- [Technology Stack](#️-technology-stack)
- [Repository Structure](#-repository-structure)
- [Setup & Configuration](#-setup--configuration)

---

## 📖 Context

Bunzl needed to:

- Extract CRM deal data from **Pipedrive** (deals, organizations, pipelines, stages, users)
- Consolidate everything in **Google BigQuery**
- Build analytical dashboards in **Power BI**

The solution is built on the **Nekt platform** using a clear **Medallion Architecture** (Bronze → Silver → Gold).

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────┐
│           DATA SOURCES           │
│          Pipedrive CRM API       │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│    BRONZE  (Raw Extraction)      │
│     Nekt Native REST Source      │
│  Authenticated API ingestion,    │
│  raw data stored as-is           │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│   SILVER  (Transformation)       │
│   Nekt PySpark Notebook          │
│  Cleaning, typing, deduplication,│
│  flattening, joins               │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     GOLD  (Analytical Load)      │
│   Nekt Native Destination        │
│  Automated write to BigQuery     │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│         Google BigQuery          │
│      Analytical Datasets         │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│            Power BI              │
│         BI / Reporting           │
└──────────────────────────────────┘
```

---

### 🥉 Bronze — Extraction

- Nekt's **native REST source** connector (no custom HTTP code)
- Authenticated connection to the Pipedrive API
- Raw data stored without transformations
- Every execution is automatically traceable via the Nekt platform

### 🥈 Silver — Transformation

- PySpark notebook executed inside Nekt ([`src/nekt_notebook.py`](src/nekt_notebook.py))
- Filters soft-deleted deals (`is_deleted = false`)
- Type casting to canonical types (string, integer, boolean)
- Nested struct flattening via dot notation and `.getField()` for hex-keyed custom fields
- Left-joins: deals → organizations, pipelines, stages, users
- Audit column `_loaded_at` (`current_timestamp`) appended to every table

### 🥇 Gold — Load

- Nekt's **native destination** connector writes directly to BigQuery
- No custom write code — fully managed by the platform
- Output tables are BI-ready, structured for direct Power BI consumption

---

## 📦 Data Catalog

### Pipedrive — Bronze

| Table Name | Description |
|---|---|
| `bunzl_pipedrive_bronze_deals` | Raw deal records |
| `bunzl_pipedrive_bronze_organizations` | Raw organization (company) records |
| `bunzl_pipedrive_bronze_pipelines` | Pipeline definitions |
| `bunzl_pipedrive_bronze_stages` | Stage definitions per pipeline |
| `bunzl_pipedrive_bronze_users` | CRM user records |
| `bunzl_pipedrive_bronze_activities` | Activity log per deal/org/person |
| `bunzl_pipedrive_bronze_notes` | Notes attached to deals/orgs/persons |
| `bunzl_pipedrive_bronze_persons` | Contact person records |
| `bunzl_pipedrive_bronze_deal_fields` | Custom field metadata for deals |
| `bunzl_pipedrive_bronze_organization_fields` | Custom field metadata for organizations |
| `bunzl_pipedrive_bronze_person_fields` | Custom field metadata for persons |

### Pipedrive — Silver

| Table Name | Key Fields | Notes |
|---|---|---|
| `bunzl_pipedrive_silver_data` | `deal_id`, `deal_add_time`, `deal_origin`, `deal_origin_id`, `deal_channel`, `deal_channel_id`, `deal_title`, `deal_value`, `deal_currency`, `deal_status`, `deal_lost_reason`, `deal_probability`, `deal_*_time`, `pipeline_id`, `pipeline_name`, `stage_id`, `stage_name`, `owner_id`, `owner_name`, `owner_email`, `org_id`, `org_name`, `org_cnpj`, `org_address_*`, `_loaded_at` | Denormalized deal grain; left-joins with organizations, pipelines, stages, users; filters `is_deleted = false`; `org_cnpj` sourced from `custom_fields` hex key `e1897931095ea6e1bba9b0ebdecfb6ad7587ec27` (organization_fields id 26) |

Full bronze schema reference: [`docs/bronze_schemas.md`](docs/bronze_schemas.md)

---

## 🔧 Notebook: Transformation Logic

**File:** [`src/nekt_notebook.py`](src/nekt_notebook.py)

Structured in three sections: **Imports**, **Extracting**, and **Transforming/Loading**.

### Helper Functions

```python
extract_nekt_table(layer_name, table_name) -> DataFrame
```
Thin wrapper over `nekt.load_table()`.

```python
save_nekt_table(df, layer_name, table_name, folder_name=None)
```
Thin wrapper over `nekt.save_table()`.

```python
ms_to_timestamp(col_name) -> Column
```
Converts Unix millisecond epoch to `TimestampType`.

```python
last_element(array_col, field) -> Column
```
Safely retrieves a named field from the last element of an array column.

---

### Pipedrive Transformations

**Silver Data** — Filters out deleted deals (`is_deleted = false`) and rows missing `id` or `add_time`. Performs four left-joins to enrich each deal with:
- organization name, CNPJ, and address breakdown (via `deals.org_id = organizations.id`)
- pipeline name (via `deals.pipeline_id = pipelines.id`)
- stage name (via `deals.stage_id = stages.id`)
- owner name and email (via `deals.owner_id = users.id`)

The `org_cnpj` field accesses `organizations.custom_fields` using `.getField("e1897931095ea6e1bba9b0ebdecfb6ad7587ec27")` (organization_fields id 26). Deduplicates on `deal_id`.

---

## 🛠️ Technology Stack

| Layer | Technology |
|---|---|
| Source system | Pipedrive CRM REST API |
| Orchestration & connectors | [Nekt](https://nekt.com.br) Data Engineering Platform |
| Transformation runtime | PySpark (via Nekt Notebook) |
| Infrastructure | Docker, GCP |
| Analytical storage | Google BigQuery |
| BI layer | Power BI |

---

## 📁 Repository Structure

```
bunzl-nekt-engineering/
├── src/
│   ├── nekt_notebook.py        # Main PySpark transformation notebook (Bronze → Silver)
│   └── local_notebook.ipynb    # Local Jupyter notebook for development/testing
├── docs/
│   └── bronze_schemas.md       # Bronze table schemas and custom field mappings
├── .env.example                # Environment variable template
└── README.md
```

---

## ⚙️ Setup & Configuration

### 1. Environment Variables

Copy `.env.example` and fill in your Nekt credentials:

```bash
cp .env.example .env
```

```env
NEKT_DATA_ACCESS_TOKEN=<your Nekt data access token>
```

### 2. Running the Notebook Locally

The `src/local_notebook.ipynb` can be used for local development. Requires a Python environment with PySpark and the Nekt SDK installed (see `pyproject.toml`).

### 3. Running in Nekt

The main transformation step (`src/nekt_notebook.py`) is executed directly inside the Nekt platform as a scheduled notebook.

**Pipeline execution order:**

1. **Nekt Source** → REST connector against Pipedrive API → writes Bronze tables
2. **Nekt Notebook** → executes `nekt_notebook.py` → reads Bronze, writes Silver tables
3. **Nekt Destination** → reads Silver tables → writes to Google BigQuery Gold datasets
