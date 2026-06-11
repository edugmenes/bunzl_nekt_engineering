# CLAUDE.md — Bunzl Nekt Engineering

## Project Purpose

This is a **data engineering pipeline** for the Bunzl client, built on the [Nekt](https://nekt.com.br) platform. It extracts CRM data from **Pipedrive**, transforms it with **PySpark**, and loads it into **Google BigQuery** for **Power BI** dashboards.

Architecture: **Medallion (Bronze → Silver → Gold)**
- Bronze: raw API data ingested by Nekt's native REST connector
- Silver: PySpark transformations (this repo)
- Gold: BigQuery tables written by Nekt's native destination connector

---

## Key Files

| File | Purpose |
|---|---|
| `src/nekt_notebook.py` | Main ETL — runs inside the Nekt platform. No setup cell needed (SDK is pre-initialized). |
| `src/local_notebook.ipynb` | Same ETL as a Jupyter notebook with a setup cell for local development. |
| `docs/bronze_schemas.md` | Full schema reference for all Pipedrive bronze tables. |
| `.env` | Local credentials (not committed). Contains `NEKT_DATA_ACCESS_TOKEN`. |
| `.env.example` | Template for `.env`. |

---

## ETL Pattern

Both notebooks follow the same three-section structure:

```
1. IMPORTS          — pyspark, nekt, typing
2. EXTRACTING       — extract_nekt_table("Bronze", "table_name")
3. TRANSFORMING     — PySpark: .filter() → .join() → .select() → .dropDuplicates()
4. LOADING          — save_nekt_table(df, "Silver", "table_name", "folder_name")
```

### Helper functions (defined in both notebooks)

- `extract_nekt_table(layer, table)` — wraps `nekt.load_table()`
- `save_nekt_table(df, layer, table, folder)` — wraps `nekt.save_table()`
- `ms_to_timestamp(col)` — Unix ms → TimestampType
- `last_element(array_col, field)` — safe last-element access from array struct

### Naming conventions

- Bronze tables: `bunzl_<source>_bronze_<entity>`
- Silver tables: `bunzl_<source>_silver_<entity>`
- Silver folder: `bunzl_<source>_silver`
- DataFrame variables: `df_bronze_<entity>`, `df_silver_<entity>`

---

## Current Silver Tables

### `bunzl_pipedrive_silver_data`

Denormalized, one row per active deal. Sources:

| Bronze Table | Join Key | Alias |
|---|---|---|
| `bunzl_pipedrive_bronze_deals` | (main) | `d` |
| `bunzl_pipedrive_bronze_organizations` | `d.org_id = o.id` | `o` |
| `bunzl_pipedrive_bronze_pipelines` | `d.pipeline_id = p.id` | `p` |
| `bunzl_pipedrive_bronze_stages` | `d.stage_id = s.id` | `s` |
| `bunzl_pipedrive_bronze_users` | `d.owner_id = u.id` | `u` |

Filter: `d.id IS NOT NULL AND d.add_time IS NOT NULL AND d.is_deleted = false`

#### `org_cnpj` field

Extracted from `organizations.custom_fields` using:
```python
F.col("o.custom_fields").getField("e1897931095ea6e1bba9b0ebdecfb6ad7587ec27")
```
This hex key corresponds to `organization_fields` id=26, name="CNPJ".

---

## Local Development

1. Requires Python 3.11 and the packages in `pyproject.toml` (use `uv sync`)
2. Copy `.env.example` to `.env` and add your `NEKT_DATA_ACCESS_TOKEN`
3. Open `src/local_notebook.ipynb` in Jupyter
4. The notebook will use the Nekt SDK to read Bronze tables and write Silver tables to the Nekt platform

---

## Adding New Silver Tables

1. Add `extract_nekt_table(...)` calls in the EXTRACTING section
2. Add the PySpark transformation following the `.filter → .join → .select → .dropDuplicates` pattern
3. Add `save_nekt_table(...)` in the LOADING section
4. Keep both `nekt_notebook.py` and `local_notebook.ipynb` in sync (same logic, different format)
5. Document new bronze tables in `docs/bronze_schemas.md`
6. Update the Data Catalog in `README.md`
