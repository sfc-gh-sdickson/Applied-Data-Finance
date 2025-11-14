![Snowflake Logo](Snowflake_Logo.svg)

# Applied Data Finance Snowflake Intelligence Agent

This package replicates the Axon Demo SI pattern for the Applied Data Finance lending business. Run the assets in order to provision databases, load synthetic data, build semantic layers, publish Cortex Search services, register ML models, and deploy the ADF Intelligence Agent.

## 1. Prerequisites

- Snowflake role with `ACCOUNTADMIN`-level privileges (or equivalent grants)
- Snowsight access for running SQL scripts and notebooks
- Warehouses with Snowpark and Cortex features enabled

## 2. Execution Order

| Step | File | Purpose |
|------|------|---------|
| 1 | `sql/setup/01_database_and_schema.sql` | Creates `ADF_INTELLIGENCE`, `RAW`, `ANALYTICS`, and warehouse `ADF_SI_WH` |
| 2 | `sql/setup/02_create_tables.sql` | Defines customer, lending, servicing, and unstructured tables |
| 3 | `sql/data/03_generate_synthetic_data.sql` | Populates synthetic data using Snowflake generators |
| 4 | `sql/views/04_create_views.sql` | Creates curated analytical views |
| 5 | `sql/views/05_create_semantic_views.sql` | Creates Cortex Analyst semantic views (syntax verified at `docs/create_semantic_view.html`) |
| 6 | `sql/search/06_create_cortex_search.sql` | Enables change tracking and creates Cortex Search services (syntax verified at `docs/create_cortex_search_content.html`) |
| 7 | `notebooks/adf_ml_models.ipynb` | Trains and registers three ML models |
| 8 | `sql/ml/07_create_model_wrapper_functions.sql` | Wraps registry models in Snowflake procedures |
| 9 | `sql/agent/08_create_intelligence_agent.sql` | Grants privileges and creates the ADF Intelligence Agent |

## 3. Data Generation Notes

- All synthetic data is created directly inside Snowflake using `TABLE(GENERATOR())`, `UNIFORM`, and `ARRAY_CONSTRUCT` functions (Step 3).
- No external files are required; run Step 3 once per environment.
- If you prefer CSV-based loading, plug your own `COPY INTO` logic before running Steps 4-9.

## 4. Cortex Search & Unstructured Data

- `SUPPORT_TRANSCRIPTS`, `POLICY_DOCUMENTS`, and `INCIDENT_REPORTS` tables have change tracking enabled prior to indexing.
- Each `CREATE OR REPLACE CORTEX SEARCH SERVICE ... ON <text_column> ATTRIBUTES ... WAREHOUSE = ADF_SI_WH TARGET_LAG = '1 hour' AS SELECT ...` statement follows the Snowflake SQL reference exactly (see `docs/create_cortex_search_content.html`).
- Query services from the SI agent or via `SELECT * FROM TABLE(SNOWFLAKE.CORTEX_SEARCH(...))` once deployed.

## 5. Cortex Analyst Setup

- Semantic views expose borrower, servicing, and support domains with verified clause ordering (`TABLES`, `RELATIONSHIPS`, `DIMENSIONS`, `METRICS`).
- Grant `SNOWFLAKE.CORTEX_ANALYST_USER` plus `REFERENCES, SELECT` on each semantic view before creating the agent (Step 9).

## 6. Notebook & ML Models

- Import `notebooks/adf_ml_models.ipynb` into Snowsight and apply the included packages (defined inside the first cell requirements).
- Models registered: `PAYMENT_VOLUME_FORECASTER`, `BORROWER_RISK_MODEL`, `COLLECTION_SUCCESS_MODEL`.
- After notebook execution, run `sql/ml/07_create_model_wrapper_functions.sql` to expose the models as stored procedures for the agent.

## 7. Verification Links

- Semantic View syntax: `docs/create_semantic_view.html`
- Cortex Search syntax: `docs/create_cortex_search_content.html`

## 8. Testing Checklist

1. `SHOW TABLES IN ADF_INTELLIGENCE.RAW;`
2. `SELECT COUNT(*) FROM ADF_INTELLIGENCE.RAW.LOAN_ACCOUNTS;`
3. `SHOW SEMANTIC VIEWS IN SCHEMA ADF_INTELLIGENCE.ANALYTICS;`
4. `SHOW CORTEX SEARCH SERVICES IN SCHEMA ADF_INTELLIGENCE.RAW;`
5. `SHOW MODELS IN SCHEMA ADF_INTELLIGENCE.ANALYTICS;`
6. `CHAT` with `ADF_INTELLIGENCE_AGENT` in Snowsight.

## 9. Support

If `CREATE CORTEX ANALYST` or other Cortex docs are inaccessible, provide the doc to the project so syntax can be verified before writing SQL, per the Applied Data Finance governance rules.
