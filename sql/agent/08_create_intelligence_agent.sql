-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - Creation Script
-- ============================================================================
-- Based on Axon Demo/sql/agent/08_create_intelligence_agent.sql
-- Execution order: run after setup, tables, data, views, semantic views,
-- Cortex Search, and ML wrapper procedures are in place.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ADF_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ADF_SI_WH;

-- ============================================================================
-- Step 1: Grant Required Privileges
-- (Adjust role SYSADMIN below if you use a different primary role)
-- ============================================================================
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_ANALYST_USER TO ROLE SYSADMIN;

GRANT USAGE ON DATABASE ADF_INTELLIGENCE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA ADF_INTELLIGENCE.ANALYTICS TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA ADF_INTELLIGENCE.RAW TO ROLE SYSADMIN;

GRANT REFERENCES, SELECT ON SEMANTIC VIEW ADF_INTELLIGENCE.ANALYTICS.SV_BORROWER_LOAN_INTELLIGENCE TO ROLE SYSADMIN;
GRANT REFERENCES, SELECT ON SEMANTIC VIEW ADF_INTELLIGENCE.ANALYTICS.SV_SERVICING_COLLECTIONS_INTELLIGENCE TO ROLE SYSADMIN;
GRANT REFERENCES, SELECT ON SEMANTIC VIEW ADF_INTELLIGENCE.ANALYTICS.SV_CUSTOMER_SUPPORT_INTELLIGENCE TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE ADF_SI_WH TO ROLE SYSADMIN;

GRANT USAGE ON CORTEX SEARCH SERVICE ADF_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS_SEARCH TO ROLE SYSADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE ADF_INTELLIGENCE.RAW.POLICY_DOCUMENTS_SEARCH TO ROLE SYSADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE ADF_INTELLIGENCE.RAW.INCIDENT_REPORTS_SEARCH TO ROLE SYSADMIN;

GRANT USAGE ON PROCEDURE ADF_INTELLIGENCE.ANALYTICS.PREDICT_PAYMENT_VOLUME(VARIANT) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE ADF_INTELLIGENCE.ANALYTICS.PREDICT_BORROWER_RISK(VARIANT) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE ADF_INTELLIGENCE.ANALYTICS.PREDICT_COLLECTION_SUCCESS(VARIANT) TO ROLE SYSADMIN;

-- ============================================================================
-- Step 2: Create the Intelligence Agent
-- ============================================================================
CREATE OR REPLACE AGENT ADF_INTELLIGENCE_AGENT
  COMMENT = 'Applied Data Finance SI agent for lending, servicing, and support insights'
  PROFILE = '{"display_name": "ADF Intelligence Agent", "avatar": "credit-card.png", "color": "teal"}'
  FROM SPECIFICATION
  $$
models:
  orchestration: auto

orchestration:
  budget:
    seconds: 60
    tokens: 32000

instructions:
  response: 'You are the Applied Data Finance analytics copilot. Use semantic views for structured lending data, Cortex Search for policies/transcripts/incidents, and ML tools for predictions. Keep answers specific, cite data, and explain how results impact lending operations.'
  orchestration: 'Favor structured SQL via Cortex Analyst when questions involve metrics. Use search services for policy or transcript lookups, and call ML procedures for forecasts or risk scoring.'
  system: 'Provide insights on borrower onboarding, loan performance, delinquency management, collections, and support interactions across ADF datasets.'
  sample_questions:
    - question: 'How many active borrowers do we have by risk tier?'
      answer: 'I will query the borrower loan intelligence semantic view and group by risk segment.'
    - question: 'What is the total outstanding principal on delinquent loans?'
      answer: 'I will use the servicing collections semantic view to sum outstanding_principal where status is delinquent.'
    - question: 'Show me recent support transcripts about hardship plans.'
      answer: 'I will call the SupportTranscriptsSearch Cortex service filtered for hardship.'
    - question: 'Predict payment volume for the next 6 months.'
      answer: 'I will run the PredictPaymentVolume tool with months_ahead = 6.'
    - question: 'Which delinquency bucket has the highest promise-to-pay success?'
      answer: 'I will analyze collections metrics in the servicing semantic view.'
    - question: 'Summarize incidents related to compliance breaches last quarter.'
      answer: 'I will query the incident search service and the servicing semantic view for counts.'
    - question: 'Identify borrowers at highest default risk in the near-prime segment.'
      answer: 'I will use the PredictBorrowerRisk tool with segment filter NEAR_PRIME.'
    - question: 'List policy documents covering hardship extensions.'
      answer: 'I will call PolicyDocumentsSearch with keywords hardship.'
    - question: 'What is our auto-pay enrollment rate?'
      answer: 'I will reference the borrower loan semantic view for auto_pay_enabled metrics.'
    - question: 'Predict promise-to-pay conversion for 60-day delinquent loans.'
      answer: 'I will run PredictCollectionSuccess with delinquency bucket 60_DPD.'

tools:
  - tool_spec:
      type: 'cortex_analyst_text_to_sql'
      name: 'BorrowerLoanAnalyst'
      description: 'Structured analysis of borrower demographics, applications, loans, and payments'
  - tool_spec:
      type: 'cortex_analyst_text_to_sql'
      name: 'ServicingCollectionsAnalyst'
      description: 'Structured analysis of delinquency, collections, and incidents'
  - tool_spec:
      type: 'cortex_analyst_text_to_sql'
      name: 'CustomerSupportAnalyst'
      description: 'Structured analysis of customer interactions and support metrics'
  - tool_spec:
      type: 'cortex_search'
      name: 'SupportTranscriptsSearch'
      description: 'Search across borrower support transcripts'
  - tool_spec:
      type: 'cortex_search'
      name: 'PolicyDocumentsSearch'
      description: 'Search across policy and compliance documents'
  - tool_spec:
      type: 'cortex_search'
      name: 'IncidentReportsSearch'
      description: 'Search risk/compliance incident investigations'
  - tool_spec:
      type: 'generic'
      name: 'PredictPaymentVolume'
      description: 'Forecasts payment volume trajectory'
      input_schema:
        type: 'object'
        properties:
          months_ahead:
            type: 'integer'
            description: 'Number of months to forecast (1-12)'
        required: ['months_ahead']
  - tool_spec:
      type: 'generic'
      name: 'PredictBorrowerRisk'
      description: 'Estimates borrower delinquency/churn risk by segment'
      input_schema:
        type: 'object'
        properties:
          risk_segment:
            type: 'string'
            description: 'Risk segment filter (e.g., PRIME, NEAR_PRIME)'
        required: ['risk_segment']
  - tool_spec:
      type: 'generic'
      name: 'PredictCollectionSuccess'
      description: 'Predicts collection promise-to-pay success probability'
      input_schema:
        type: 'object'
        properties:
          delinquency_bucket:
            type: 'string'
            description: 'Delinquency bucket filter (e.g., 30_DPD, 60_DPD)'
        required: ['delinquency_bucket']

tool_resources:
  BorrowerLoanAnalyst:
    semantic_view: 'ADF_INTELLIGENCE.ANALYTICS.SV_BORROWER_LOAN_INTELLIGENCE'
    execution_environment:
      type: 'warehouse'
      warehouse: 'ADF_SI_WH'
      query_timeout: 60
  ServicingCollectionsAnalyst:
    semantic_view: 'ADF_INTELLIGENCE.ANALYTICS.SV_SERVICING_COLLECTIONS_INTELLIGENCE'
    execution_environment:
      type: 'warehouse'
      warehouse: 'ADF_SI_WH'
      query_timeout: 60
  CustomerSupportAnalyst:
    semantic_view: 'ADF_INTELLIGENCE.ANALYTICS.SV_CUSTOMER_SUPPORT_INTELLIGENCE'
    execution_environment:
      type: 'warehouse'
      warehouse: 'ADF_SI_WH'
      query_timeout: 60
  SupportTranscriptsSearch:
    search_service: 'ADF_INTELLIGENCE.RAW.SUPPORT_TRANSCRIPTS_SEARCH'
    max_results: 8
    title_column: 'interaction_id'
    id_column: 'transcript_id'
  PolicyDocumentsSearch:
    search_service: 'ADF_INTELLIGENCE.RAW.POLICY_DOCUMENTS_SEARCH'
    max_results: 5
    title_column: 'title'
    id_column: 'policy_id'
  IncidentReportsSearch:
    search_service: 'ADF_INTELLIGENCE.RAW.INCIDENT_REPORTS_SEARCH'
    max_results: 8
    title_column: 'incident_type'
    id_column: 'incident_report_id'
  PredictPaymentVolume:
    type: 'procedure'
    identifier: 'ADF_INTELLIGENCE.ANALYTICS.PREDICT_PAYMENT_VOLUME'
    execution_environment:
      type: 'warehouse'
      warehouse: 'ADF_SI_WH'
      query_timeout: 60
  PredictBorrowerRisk:
    type: 'procedure'
    identifier: 'ADF_INTELLIGENCE.ANALYTICS.PREDICT_BORROWER_RISK'
    execution_environment:
      type: 'warehouse'
      warehouse: 'ADF_SI_WH'
      query_timeout: 60
  PredictCollectionSuccess:
    type: 'procedure'
    identifier: 'ADF_INTELLIGENCE.ANALYTICS.PREDICT_COLLECTION_SUCCESS'
    execution_environment:
      type: 'warehouse'
      warehouse: 'ADF_SI_WH'
      query_timeout: 60
  $$;

-- ============================================================================
-- Step 3: Verify & Grant Agent Usage
-- ============================================================================
SHOW AGENTS LIKE 'ADF_INTELLIGENCE_AGENT';
DESCRIBE AGENT ADF_INTELLIGENCE_AGENT;

GRANT USAGE ON AGENT ADF_INTELLIGENCE_AGENT TO ROLE SYSADMIN;

-- ============================================================================
-- Step 4: Reference Notes
-- ============================================================================
-- Use Snowsight > AI & ML > Agents to test ADF_INTELLIGENCE_AGENT.
-- Example prompts: 'Show delinquency distribution by product',
-- 'Search policies for hardship extensions', 'Predict payment volume next quarter'.
