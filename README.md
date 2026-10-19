\# Global Retail Analytics Platform



End-to-end data engineering project on the Olist Brazilian E-Commerce dataset,

built with a Medallion Architecture (Bronze → Silver → Gold).



\## Business Problem

Olist sellers and analysts need reliable, quality-checked data to track revenue,

customer behavior, and seller performance across 100K+ orders.



\## Architecture



```mermaid

flowchart TD

&#x20;   A\[Kaggle Olist CSVs<br/>+ simulated streaming orders] --> B\[Microsoft Fabric<br/>Bronze: raw ingestion in OneLake]

&#x20;   B --> C\[Databricks + PySpark<br/>Silver: cleaned Delta tables]

&#x20;   C --> D\[Snowflake<br/>Gold: star schema, SCD Type 2]

&#x20;   D --> E\[dbt<br/>models, tests, docs]

&#x20;   E --> F\[Power BI<br/>dashboards]

&#x20;   G\[GitHub Actions<br/>CI/CD] -.-> E

&#x20;   G -.-> C

```



\## Tech Stack

Python · SQL · Microsoft Fabric · Databricks · PySpark · Delta Lake · Snowflake · dbt · Great Expectations · Pytest · GitHub Actions · Power BI



\## Project Structure

| Folder | Purpose |

|---|---|

| `ingestion/` | Fabric pipelines and ingestion scripts (Bronze) |

| `transformation/` | Databricks notebooks (Silver) |

| `warehouse/` | Snowflake DDL and load scripts (Gold) |

| `dbt\_project/` | dbt models, tests, docs |

| `ci\_cd/` | CI/CD helper scripts |

| `tests/` | Pytest and Great Expectations |

| `docs/` | Diagrams and data dictionary |

| `scripts/` | Utility scripts |



\## Status

🚧 In progress. Week 1 of 9: project setup.



\## Branching

`feature/\*` → `dev` → `staging` → `main`

