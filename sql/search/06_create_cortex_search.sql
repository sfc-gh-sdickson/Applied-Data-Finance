-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - Cortex Search Services
-- ============================================================================
-- Syntax confirmed via docs/create_cortex_search_content.html.
-- Prereq tables: SUPPORT_TRANSCRIPTS, POLICY_DOCUMENTS, INCIDENT_REPORTS
-- ============================================================================

USE DATABASE ADF_INTELLIGENCE;
USE SCHEMA RAW;
USE WAREHOUSE ADF_SI_WH;

-- ============================================================================
-- Step 1: Ensure Change Tracking (required before CREATE CORTEX SEARCH SERVICE)
-- ============================================================================
ALTER TABLE SUPPORT_TRANSCRIPTS SET CHANGE_TRACKING = TRUE;
ALTER TABLE POLICY_DOCUMENTS SET CHANGE_TRACKING = TRUE;
ALTER TABLE INCIDENT_REPORTS SET CHANGE_TRACKING = TRUE;

-- ============================================================================
-- Step 2: (Optional) Seed extra unstructured content
-- ============================================================================
INSERT INTO POLICY_DOCUMENTS (policy_id, title, content, document_category, business_unit, owner,
                              effective_date, revision, keywords, created_at, last_updated)
SELECT
    'POLICY004',
    'Hardship & Forbearance FAQ',
    $$FAQ covering eligibility checks, documentation requirements, and regulatory disclosures for hardship and forbearance programs.$$,
    'SERVICING',
    'OPERATIONS',
    'Borrower Assistance Center',
    '2025-05-01',
    'v1.0',
    'hardship, forbearance, faq',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
WHERE NOT EXISTS (SELECT 1 FROM POLICY_DOCUMENTS WHERE policy_id = 'POLICY004');

-- ============================================================================
-- Step 3: Create Cortex Search Service for Support Transcripts
-- ============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE SUPPORT_TRANSCRIPTS_SEARCH
  ON transcript_text
  ATTRIBUTES customer_id, loan_id, interaction_channel, issue_category, resolution_status
  WAREHOUSE = ADF_SI_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Semantic search across borrower support transcripts for servicing insights'
AS
  SELECT
    transcript_id,
    transcript_text,
    interaction_id,
    customer_id,
    loan_id,
    interaction_channel,
    transcript_date,
    issue_category,
    resolution_status,
    created_at
  FROM SUPPORT_TRANSCRIPTS;

-- ============================================================================
-- Step 4: Create Cortex Search Service for Policy Documents
-- ============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE POLICY_DOCUMENTS_SEARCH
  ON content
  ATTRIBUTES document_category, business_unit, title, keywords
  WAREHOUSE = ADF_SI_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Search over policy/procedure docs to answer compliance and ops questions'
AS
  SELECT
    policy_id,
    content,
    title,
    document_category,
    business_unit,
    owner,
    effective_date,
    revision,
    keywords,
    last_updated
  FROM POLICY_DOCUMENTS;

-- ============================================================================
-- Step 5: Create Cortex Search Service for Incident / Risk Reports
-- ============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE INCIDENT_REPORTS_SEARCH
  ON report_text
  ATTRIBUTES loan_id, customer_id, incident_type, severity, status
  WAREHOUSE = ADF_SI_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Searchable repository of risk/compliance/servicing incident investigations'
AS
  SELECT
    incident_report_id,
    report_text,
    loan_id,
    customer_id,
    incident_type,
    severity,
    status,
    findings_summary,
    recommendations,
    report_date,
    investigator
  FROM INCIDENT_REPORTS;

-- ============================================================================
-- Step 6: Status Summary
-- ============================================================================
SELECT
    'ADF Cortex Search services created successfully' AS status,
    (SELECT CHANGE_TRACKING FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SUPPORT_TRANSCRIPTS') AS transcripts_tracking,
    (SELECT CHANGE_TRACKING FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'POLICY_DOCUMENTS') AS policies_tracking,
    (SELECT CHANGE_TRACKING FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'INCIDENT_REPORTS') AS incidents_tracking;
