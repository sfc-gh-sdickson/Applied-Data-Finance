<img src="../Snowflake_Logo.svg" alt="Snowflake Logo" width="33%">

# Applied Data Finance Intelligence Agent Setup Guide

This mirrors the Axon Demo `AGENT_SETUP.md` format but targets the Applied Data Finance (ADF) lending use case. Follow every step in order—stop if any command fails. All SQL syntax is verified against the Snowflake documentation provided in `docs/`.

---
## 0. Requirements
- Snowflake role with privileges to create databases, warehouses, Cortex Search services, semantic views, notebooks, Model Registry entries, and agents (e.g., `ACCOUNTADMIN`).
- Snowflake features: Snowpark (Python), Cortex Analyst, Cortex Search, Model Registry.
- Local copy of this repository at `/Users/sdickson/Applied Data Finance`.
- Documentation references on hand:
  - `docs/create_semantic_view.html`
  - `docs/create_cortex_search_content.html`

---
## 1. Initialize Database, Schemas, and Warehouse
**File:** `sql/setup/01_database_and_schema.sql`
1. Open a Snowsight worksheet (or SnowSQL) with role `ACCOUNTADMIN`.
2. Run the file contents.
3. Verify:
   ```sql
   SHOW DATABASES LIKE 'ADF_INTELLIGENCE';
   SHOW WAREHOUSES LIKE 'ADF_SI_WH';
   ```

---
## 2. Create Core Tables
**File:** `sql/setup/02_create_tables.sql`
1. Same worksheet, still using `ADF_INTELLIGENCE` context.
2. Run the file.
3. Verify representative tables:
   ```sql
   SHOW TABLES IN ADF_INTELLIGENCE.RAW;
   ```

---
## 3. Generate Synthetic Data
**File:** `sql/data/03_generate_synthetic_data.sql`
1. Execute the script—this uses only Snowflake generators (`TABLE(GENERATOR())`).
2. Confirm counts from the closing query; expect non-zero rows for each table.
3. Optional spot checks:
   ```sql
   SELECT COUNT(*) FROM ADF_INTELLIGENCE.RAW.CUSTOMERS;
   SELECT COUNT(*) FROM ADF_INTELLIGENCE.RAW.LOAN_ACCOUNTS;
   ```

---
## 4. Build Analytical Views
**File:** `sql/views/04_create_views.sql`
1. Run to create `V_CUSTOMER_360`, `V_LOAN_ACCOUNT_PERFORMANCE`, etc.
2. Verify:
   ```sql
   SHOW VIEWS IN ADF_INTELLIGENCE.ANALYTICS;
   ```

---
## 5. Create Semantic Views (Cortex Analyst)
**File:** `sql/views/05_create_semantic_views.sql`
1. Confirm clause order with `docs/create_semantic_view.html` if editing.
2. Run the script—creates:
   - `SV_BORROWER_LOAN_INTELLIGENCE`
   - `SV_SERVICING_COLLECTIONS_INTELLIGENCE`
   - `SV_CUSTOMER_SUPPORT_INTELLIGENCE`
3. Verify:
   ```sql
   SHOW SEMANTIC VIEWS IN SCHEMA ADF_INTELLIGENCE.ANALYTICS;
   ```

---
## 6. Enable Unstructured Search (Cortex Search)
**File:** `sql/search/06_create_cortex_search.sql`
1. Ensures change tracking on `SUPPORT_TRANSCRIPTS`, `POLICY_DOCUMENTS`, `INCIDENT_REPORTS`.
2. Creates three Cortex Search services using the syntax in `docs/create_cortex_search_content.html`.
3. Verify:
   ```sql
   SHOW CORTEX SEARCH SERVICES IN SCHEMA ADF_INTELLIGENCE.RAW;
   ```

---
## 7. Train and Register ML Models
**Notebook:** `notebooks/adf_ml_models.ipynb`
1. In Snowsight: Projects → Notebooks → **Import** the notebook.
2. Click **Packages** → add `snowflake-ml-python`, `scikit-learn`, `pandas`, `numpy` (or upload your environment spec).
3. Set context: Database `ADF_INTELLIGENCE`, Schema `ANALYTICS`, Warehouse `ADF_SI_WH`.
4. Run all cells sequentially; models registered:
   - `PAYMENT_VOLUME_FORECASTER`
   - `BORROWER_RISK_MODEL`
   - `COLLECTION_SUCCESS_MODEL`
5. Verify:
   ```sql
   SHOW MODELS IN SCHEMA ADF_INTELLIGENCE.ANALYTICS;
   ```

---
## 8. Create ML Wrapper Procedures
**File:** `sql/ml/07_create_model_wrapper_functions.sql`
1. Run after the notebook to expose the models as stored procedures:
   - `PREDICT_PAYMENT_VOLUME(INT)`
   - `PREDICT_BORROWER_RISK(VARCHAR)`
   - `PREDICT_COLLECTION_SUCCESS(VARCHAR)`
2. Optional test call:
   ```sql
   CALL PREDICT_PAYMENT_VOLUME(6);
   ```

---
## 9. Create the ADF Intelligence Agent
**File:** `sql/agent/08_create_intelligence_agent.sql`
1. Grants Cortex Analyst role, semantic view references, Cortex Search usage, and ML procedure access to `SYSADMIN` (adjust role names if needed).
2. Creates `ADF_INTELLIGENCE_AGENT` with semantic, search, and ML tools.
3. Verify:
   ```sql
   SHOW AGENTS LIKE 'ADF_INTELLIGENCE_AGENT';
   DESCRIBE AGENT ADF_INTELLIGENCE_AGENT;
   ```
4. In Snowsight: AI & ML → Agents → open `ADF_INTELLIGENCE_AGENT` → click **Chat**.

---
## 10. Validation Checklist
- `SELECT COUNT(*) FROM ADF_INTELLIGENCE.RAW.LOAN_ACCOUNTS;`
- `SHOW SEMANTIC VIEWS IN SCHEMA ADF_INTELLIGENCE.ANALYTICS;`
- `SHOW CORTEX SEARCH SERVICES IN SCHEMA ADF_INTELLIGENCE.RAW;`
- `SHOW MODELS IN SCHEMA ADF_INTELLIGENCE.ANALYTICS;`
- `CALL PREDICT_BORROWER_RISK('NEAR_PRIME');`
- Ask the agent: “How many active borrowers by risk tier?”

---
## 11. Maintenance Notes
- Re-run Step 3 when synthetic data needs refreshing.
- If models are retrained, re-execute Steps 7–9 (notebook → wrappers → agent) so the agent uses the latest versions.
- Keep the Snowflake SQL reference files in `docs/` for future syntax validation—never guess at syntax.

