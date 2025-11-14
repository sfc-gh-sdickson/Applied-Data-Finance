-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - Synthetic Data Generation
-- ============================================================================
-- Purpose: Populate RAW tables with realistic lending, servicing, and support data
-- Volume targets (~):
--   Customers 15k, Loan Apps 25k, Loan Accounts 12k, Payments 180k,
--   Collections 35k, Interactions 50k, Transcripts 8k, Policy docs 3,
--   Incident reports 5k
-- ============================================================================

USE DATABASE ADF_INTELLIGENCE;
USE SCHEMA RAW;
USE WAREHOUSE ADF_SI_WH;

-- ============================================================================
-- Step 1: Generate Customers
-- ============================================================================
INSERT INTO CUSTOMERS
WITH base AS (
    SELECT
        SEQ4() AS seq,
        UNIFORM(580, 820, RANDOM()) AS credit_score_raw,
        UNIFORM(30000, 180000, RANDOM()) / 1.0 AS income_raw,
        UNIFORM(18, 75, RANDOM()) AS age_years
    FROM TABLE(GENERATOR(ROWCOUNT => 15000))
)
SELECT
    'CUST' || LPAD(seq, 8, '0') AS customer_id,
    ARRAY_CONSTRUCT('Emily','Michael','Sophia','Noah','Isabella','Ethan','Ava','Liam','Mia','Oliver',
                    'Charlotte','Amelia','Benjamin','Emma','Lucas','Harper','Sebastian','Victoria','Daniel','Grace')[UNIFORM(0, 19, RANDOM())] AS first_name,
    ARRAY_CONSTRUCT('Johnson','Martinez','Reid','Patel','Nguyen','Garcia','Kim','Chen','Brown','Smith',
                    'Singh','Lopez','Clark','Davis','Hall','Allen','Rivera','King','Wright','Cooper')[UNIFORM(0, 19, RANDOM())] AS last_name,
    LOWER(CONCAT('cust', seq, '@applieddatafinance.com')) AS email,
    CONCAT('+1-', LPAD(UNIFORM(200, 999, RANDOM()), 3, '0'), '-', LPAD(UNIFORM(100, 999, RANDOM()), 3, '0'),
           '-', LPAD(UNIFORM(1000, 9999, RANDOM()), 4, '0')) AS phone,
    SHA2(TO_VARCHAR(seq), 256) AS ssn_hash,
    DATEADD('day', -1 * age_years * 365, CURRENT_DATE()) AS date_of_birth,
    ARRAY_CONSTRUCT('742 Maple St','18 Harbor View','2201 Central Ave','937 Hillcrest Dr','5100 Westbrook Ln')[UNIFORM(0, 4, RANDOM())] AS address_line1,
    NULL AS address_line2,
    ARRAY_CONSTRUCT('Phoenix','Dallas','Charlotte','Tampa','Chicago','Atlanta','Seattle','Denver','Austin','Cleveland')[UNIFORM(0, 9, RANDOM())] AS city,
    ARRAY_CONSTRUCT('AZ','TX','NC','FL','IL','GA','WA','CO','TX','OH')[UNIFORM(0, 9, RANDOM())] AS state,
    LPAD(UNIFORM(10000, 99999, RANDOM()), 5, '0') AS postal_code,
    'USA' AS country,
    ARRAY_CONSTRUCT('FULL_TIME','PART_TIME','SELF_EMPLOYED','UNEMPLOYED','RETIRED')[UNIFORM(0, 4, RANDOM())] AS employment_status,
    income_raw AS annual_income,
    credit_score_raw AS credit_score,
    CASE
        WHEN credit_score_raw >= 760 THEN 'PRIME'
        WHEN credit_score_raw >= 680 THEN 'NEAR_PRIME'
        ELSE 'SUB_PRIME'
    END AS risk_segment,
    ARRAY_CONSTRUCT('ENGLISH','SPANISH','FRENCH')[UNIFORM(0, 2, RANDOM())] AS preferred_language,
    ARRAY_CONSTRUCT('MOBILE_APP','WEB','CALL_CENTER','PARTNER')[UNIFORM(0, 3, RANDOM())] AS onboarding_channel,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM base;

-- ============================================================================
-- Step 2: Generate Loan Applications
-- ============================================================================
INSERT INTO LOAN_APPLICATIONS
SELECT
    'APP' || LPAD(SEQ4(), 9, '0') AS application_id,
    c.customer_id,
    ARRAY_CONSTRUCT('PERSONAL_LOAN','DEBT_CONSOLIDATION','CREDIT_BUILDER','AUTO_REPAIR','MEDICAL_FINANCING')[UNIFORM(0, 4, RANDOM())] AS product_type,
    DATEADD('day', -1 * UNIFORM(1, 720, RANDOM()), CURRENT_DATE()) AS application_date,
    (UNIFORM(2000, 50000, RANDOM()) / 1.0)::NUMBER(12,2) AS requested_amount,
    ARRAY_CONSTRUCT(24,36,48,60,72)[UNIFORM(0, 4, RANDOM())] AS term_months,
    ARRAY_CONSTRUCT('Consolidate credit cards','Pay medical expenses','Home improvement','Emergency cash','Debt payoff')[UNIFORM(0, 4, RANDOM())] AS purpose,
    ARRAY_CONSTRUCT('MOBILE','WEB','CALL_CENTER','BRANCH_PARTNER')[UNIFORM(0, 3, RANDOM())] AS channel,
    CASE
        WHEN UNIFORM(0, 100, RANDOM()) < 70 THEN 'APPROVED'
        WHEN UNIFORM(0, 100, RANDOM()) < 85 THEN 'PENDING'
        ELSE 'DECLINED'
    END AS status,
    CASE
        WHEN status = 'APPROVED' THEN DATEADD('day', UNIFORM(1, 10, RANDOM()), application_date)
        WHEN status = 'DECLINED' THEN DATEADD('day', UNIFORM(1, 5, RANDOM()), application_date)
        ELSE NULL
    END AS decision_date,
    CASE WHEN status = 'APPROVED' THEN requested_amount * (0.85 + UNIFORM(0, 15, RANDOM()) / 100) END AS approved_amount,
    ROUND(ARRAY_CONSTRUCT(12.99,14.49,15.99,17.49,18.99,21.49)[UNIFORM(0, 5, RANDOM())], 2) AS interest_rate,
    'UW' || LPAD(UNIFORM(1, 250, RANDOM()), 4, '0') AS underwriter_id,
    CASE WHEN status = 'DECLINED' THEN ARRAY_CONSTRUCT('Low credit score','High leverage','Insufficient income','Unverified identity')[UNIFORM(0, 3, RANDOM())] END AS decline_reason,
    CASE
        WHEN c.credit_score >= 760 THEN 'A'
        WHEN c.credit_score >= 700 THEN 'B'
        WHEN c.credit_score >= 640 THEN 'C'
        ELSE 'D'
    END AS risk_grade,
    (UNIFORM(600, 900, RANDOM()) / 10.0)::NUMBER(5,2) AS automation_score,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM CUSTOMERS c
JOIN TABLE(GENERATOR(ROWCOUNT => 2)) g
WHERE UNIFORM(0, 100, RANDOM()) < 85
LIMIT 25000;

-- ============================================================================
-- Step 3: Generate Funded Loan Accounts (subset of approved apps)
-- ============================================================================
INSERT INTO LOAN_ACCOUNTS
SELECT
    'LOAN' || LPAD(SEQ4(), 9, '0') AS loan_id,
    la.application_id,
    la.customer_id,
    COALESCE(la.decision_date, DATEADD('day', 3, la.application_date)) AS funding_date,
    la.approved_amount AS original_amount,
    (la.approved_amount * (0.55 + UNIFORM(0, 40, RANDOM()) / 100))::NUMBER(12,2) AS outstanding_principal,
    ROUND((la.approved_amount * la.interest_rate / 1200), 2) AS accrued_interest,
    la.product_type,
    la.term_months,
    la.interest_rate,
    ARRAY_CONSTRUCT('AMORTIZING','INTEREST_ONLY','STEP_UP')[UNIFORM(0, 2, RANDOM())] AS amortization_type,
    ARRAY_CONSTRUCT(3,8,12,15,20,25)[UNIFORM(0, 5, RANDOM())] AS payment_due_day,
    CASE
        WHEN UNIFORM(0, 100, RANDOM()) < 78 THEN 'CURRENT'
        WHEN UNIFORM(0, 100, RANDOM()) < 90 THEN 'DELINQUENT'
        ELSE 'CHARGED_OFF'
    END AS servicing_status,
    CASE
        WHEN servicing_status = 'CURRENT' THEN NULL
        WHEN UNIFORM(0, 100, RANDOM()) < 50 THEN '30_DPD'
        WHEN UNIFORM(0, 100, RANDOM()) < 80 THEN '60_DPD'
        ELSE '90_PLUS_DPD'
    END AS delinquency_bucket,
    DATEADD('month', UNIFORM(0, 1, RANDOM()), DATEADD('day', payment_due_day, DATE_TRUNC('month', CURRENT_DATE()))) AS next_payment_due,
    DATEADD('day', -1 * UNIFORM(1, 30, RANDOM()), CURRENT_DATE()) AS last_payment_date,
    DATEADD('day', -1 * UNIFORM(1, 45, RANDOM()), CURRENT_DATE()) AS last_statement_date,
    UNIFORM(0, 100, RANDOM()) < 40 AS auto_pay_enabled,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM LOAN_APPLICATIONS la
WHERE la.status = 'APPROVED'
LIMIT 12000;

-- ============================================================================
-- Step 4: Generate Payment History
-- ============================================================================
INSERT INTO PAYMENT_HISTORY
SELECT
    'PAY' || LPAD(SEQ4(), 10, '0') AS payment_id,
    loan.loan_id,
    loan.customer_id,
    DATEADD('month', -1 * UNIFORM(0, 24, RANDOM()), CURRENT_DATE()) AS payment_date,
    DATEADD('second', UNIFORM(0, 86400, RANDOM()), payment_date) AS posted_timestamp,
    ARRAY_CONSTRUCT('ACH','DEBIT_CARD','BANK_TRANSFER','CHECK')[UNIFORM(0, 3, RANDOM())] AS payment_method,
    ARRAY_CONSTRUCT('AUTOPAY','ONE_TIME','AGENT_ASSISTED')[UNIFORM(0, 2, RANDOM())] AS payment_channel,
    (loan.original_amount / loan.term_months) * (0.9 + UNIFORM(0, 25, RANDOM()) / 100) AS amount,
    amount * 0.7 AS principal_component,
    amount * 0.25 AS interest_component,
    amount * 0.05 AS fees_component,
    UNIFORM(0, 100, RANDOM()) < 15 AS late_fee_applied,
    UNIFORM(0, 100, RANDOM()) < 3 AS nsf_flag,
    FALSE AS reversal_flag,
    CURRENT_TIMESTAMP() AS created_at
FROM LOAN_ACCOUNTS loan
JOIN TABLE(GENERATOR(ROWCOUNT => 15)) g
WHERE loan.servicing_status <> 'CHARGED_OFF'
  AND UNIFORM(0, 100, RANDOM()) < 60
LIMIT 180000;

-- ============================================================================
-- Step 5: Generate Collection Events
-- ============================================================================
INSERT INTO COLLECTION_EVENTS
SELECT
    'COLL' || LPAD(SEQ4(), 9, '0') AS collection_id,
    loan.loan_id,
    loan.customer_id,
    DATEADD('day', -1 * UNIFORM(0, 120, RANDOM()), CURRENT_TIMESTAMP()) AS event_timestamp,
    ARRAY_CONSTRUCT('CALL','SMS','EMAIL','LETTER','SKIP_TRACE')[UNIFORM(0, 4, RANDOM())] AS event_type,
    'AGENT' || LPAD(UNIFORM(1, 150, RANDOM()), 4, '0') AS agent_id,
    ARRAY_CONSTRUCT('PROMISE_TO_PAY','PAYMENT_MADE','CONTACTED','NO_CONTACT','RIGHT_PARTY_CONTACT')[UNIFORM(0, 4, RANDOM())] AS outcome,
    CASE WHEN outcome IN ('PROMISE_TO_PAY','RIGHT_PARTY_CONTACT') THEN DATEADD('day', UNIFORM(1, 14, RANDOM()), CURRENT_DATE()) END AS promise_to_pay_date,
    CASE WHEN outcome = 'PROMISE_TO_PAY' THEN (UNIFORM(200, 1500, RANDOM()) / 1.0)::NUMBER(12,2) END AS promise_amount,
    'Collections note for ' || loan.loan_id || ' outcome ' || outcome AS notes,
    ARRAY_CONSTRUCT('LOW','MEDIUM','HIGH')[UNIFORM(0, 2, RANDOM())] AS severity,
    ARRAY_CONSTRUCT('Schedule follow-up','Send hardship packet','Transfer to legal','Monitor account')[UNIFORM(0, 3, RANDOM())] AS next_action,
    CURRENT_TIMESTAMP() AS created_at
FROM LOAN_ACCOUNTS loan
WHERE loan.servicing_status IN ('DELINQUENT','CHARGED_OFF')
LIMIT 35000;

-- ============================================================================
-- Step 6: Generate Customer Interactions
-- ============================================================================
INSERT INTO CUSTOMER_INTERACTIONS
SELECT
    'INT' || LPAD(SEQ4(), 10, '0') AS interaction_id,
    loan.customer_id,
    loan.loan_id,
    DATEADD('day', -1 * UNIFORM(0, 90, RANDOM()), CURRENT_TIMESTAMP()) AS interaction_date,
    ARRAY_CONSTRUCT('PAYMENT_ASSIST','HARDSHIP_REQUEST','STATUS_UPDATE','PAYOFF_QUOTE','GENERAL_SUPPORT')[UNIFORM(0, 4, RANDOM())] AS interaction_type,
    ARRAY_CONSTRUCT('PHONE','CHAT','EMAIL','SMS','PORTAL_MESSAGE')[UNIFORM(0, 4, RANDOM())] AS channel,
    ARRAY_CONSTRUCT('Ava Thompson','Carlos Perez','Lena Stewart','Marcus Hall','Priya Banerjee','Taylor Fox')[UNIFORM(0, 5, RANDOM())] AS agent_name,
    ARRAY_CONSTRUCT('Payment plan','Statement question','Auto-pay setup','Credit dispute','Loan payoff','Identity verification')[UNIFORM(0, 5, RANDOM())] AS topic,
    ARRAY_CONSTRUCT('RESOLVED','ESCALATED','FOLLOW_UP','INFO_PROVIDED')[UNIFORM(0, 3, RANDOM())] AS outcome,
    (UNIFORM(600, 950, RANDOM()) / 100.0)::NUMBER(5,2) AS sentiment_score,
    'Notes for ' || topic || ' via ' || channel || ' outcome ' || outcome AS notes,
    CASE WHEN outcome IN ('FOLLOW_UP','ESCALATED') THEN DATEADD('day', UNIFORM(2, 10, RANDOM()), CURRENT_DATE()) END AS follow_up_date,
    outcome = 'ESCALATED' AS escalation_flag,
    CURRENT_TIMESTAMP() AS created_at
FROM LOAN_ACCOUNTS loan
JOIN TABLE(GENERATOR(ROWCOUNT => 4)) g
WHERE UNIFORM(0, 100, RANDOM()) < 70
LIMIT 50000;

-- ============================================================================
-- Step 7: Generate Support Transcripts from interactions
-- ============================================================================
INSERT INTO SUPPORT_TRANSCRIPTS
SELECT
    'TRANS' || LPAD(SEQ4(), 10, '0') AS transcript_id,
    ci.interaction_id,
    ci.customer_id,
    ci.loan_id,
    CASE (ABS(RANDOM()) % 4)
        WHEN 0 THEN 'Agent: Thanks for calling Applied Data Finance, this is ' || ci.agent_name ||
            '. Borrower: I need to move my payment due date. Agent: I can offer a hardship plan that pushes it 15 days if you enroll in auto-pay.' ||
            ' Borrower: That helps, please set it up. Agent: Done. Confirmation email sent.'
        WHEN 1 THEN 'Chat log - borrower reported NSF fee. Agent verified ACH failure and waived the fee after confirming updated bank info. Notes indicate customer education on keeping funds available before cut-off.'
        WHEN 2 THEN 'Email thread regarding payoff quote. Borrower requested 10-day payoff. Agent attached PDF, explained per-diem interest, and confirmed lien release timeline.'
        ELSE 'SMS conversation about delinquency. Collections agent reminded borrower of promise-to-pay due Friday, offered payment portal link, and documented partial payment arrangement.'
    END AS transcript_text,
    ci.channel AS interaction_channel,
    ci.interaction_date AS transcript_date,
    ci.agent_name AS agent_id,
    ARRAY_CONSTRUCT('PAYMENT','HARDSHIP','COMPLAINT','GENERAL')[UNIFORM(0, 3, RANDOM())] AS issue_category,
    ARRAY_CONSTRUCT('OPEN','IN_PROGRESS','RESOLVED')[UNIFORM(0, 2, RANDOM())] AS resolution_status,
    CURRENT_TIMESTAMP() AS created_at
FROM CUSTOMER_INTERACTIONS ci
WHERE UNIFORM(0, 100, RANDOM()) < 20
LIMIT 8000;

-- ============================================================================
-- Step 8: Seed Policy Documents (static examples)
-- ============================================================================
INSERT INTO POLICY_DOCUMENTS VALUES
('POLICY001', 'ADF Loan Servicing Playbook',
 $$APPLIED DATA FINANCE SERVICING PLAYBOOK
Topics: payment processing, hardship evaluation, auto-debit controls, and default management. Includes escalation matrices and compliance checkpoints.$$,
 'SERVICING', 'OPERATIONS', 'Servicing PMO', '2025-01-01', 'v3.1', 'servicing, hardship, auto-pay',
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('POLICY002', 'Collections Quality Standards',
 $$Collections Quality Framework:
1. Call cadence by delinquency bucket
2. Consumer Financial Protection compliance steps
3. Right-party contact verification checklist
4. Promise-to-pay documentation requirements$$,
 'COLLECTIONS', 'RISK', 'Head of Collections', '2024-09-01', 'v2.4', 'collections, compliance, QA',
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
('POLICY003', 'Borrower Communications & Disclosures',
 $$Guidelines for omnichannel outreach, language access, hardship scripting, and documentation retention.
Covers CFPB disclosure templates and adverse action wording.$$,
 'COMMS', 'LEGAL', 'Compliance Office', '2025-04-15', 'v1.7', 'disclosures, communications, compliance',
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- ============================================================================
-- Step 9: Generate Incident / Risk Reports
-- ============================================================================
INSERT INTO INCIDENT_REPORTS
SELECT
    'INC' || LPAD(SEQ4(), 9, '0') AS incident_report_id,
    loan.loan_id,
    loan.customer_id,
    'Incident summary for loan ' || loan.loan_id || ': ' ||
    CASE (ABS(RANDOM()) % 4)
        WHEN 0 THEN 'Payment posting delay detected. Root cause traced to partner bank outage. Corrective action: enable multi-bank redundancy.'
        WHEN 1 THEN 'Complaint about inaccurate payoff quote. Investigation found stale escrow balance. Corrected and customer credited $25 goodwill.'
        WHEN 2 THEN 'Regulatory breach review: call recording missing disclosure. Agent retrained; QA sampling increased for 30 days.'
        ELSE 'Hardship workflow exception triggered. Borrower received incorrect plan length. Systems team patched rule engine and reissued documents.'
    END AS report_text,
    ARRAY_CONSTRUCT('SERVICING','COLLECTIONS','COMPLIANCE','TECH_INCIDENT')[UNIFORM(0, 3, RANDOM())] AS incident_type,
    ARRAY_CONSTRUCT('LOW','MEDIUM','HIGH')[UNIFORM(0, 2, RANDOM())] AS severity,
    ARRAY_CONSTRUCT('OPEN','IN_REVIEW','CLOSED')[UNIFORM(0, 2, RANDOM())] AS status,
    'Key findings documented for ' || incident_type || ' severity ' || severity AS findings_summary,
    'Recommendations: enhance monitoring, retrain staff, automate controls' AS recommendations,
    DATEADD('day', -1 * UNIFORM(0, 45, RANDOM()), CURRENT_TIMESTAMP()) AS report_date,
    ARRAY_CONSTRUCT('Risk Office','Compliance','Servicing QA')[UNIFORM(0, 2, RANDOM())] AS investigator,
    CURRENT_TIMESTAMP() AS created_at
FROM LOAN_ACCOUNTS loan
WHERE UNIFORM(0, 100, RANDOM()) < 40
LIMIT 5000;

-- ============================================================================
-- Completion Summary
-- ============================================================================
SELECT
    'ADF synthetic data generation completed' AS STATUS,
    (SELECT COUNT(*) FROM CUSTOMERS) AS customers,
    (SELECT COUNT(*) FROM LOAN_APPLICATIONS) AS loan_applications,
    (SELECT COUNT(*) FROM LOAN_ACCOUNTS) AS loan_accounts,
    (SELECT COUNT(*) FROM PAYMENT_HISTORY) AS payment_history_rows,
    (SELECT COUNT(*) FROM COLLECTION_EVENTS) AS collection_events,
    (SELECT COUNT(*) FROM CUSTOMER_INTERACTIONS) AS interactions,
    (SELECT COUNT(*) FROM SUPPORT_TRANSCRIPTS) AS transcripts,
    (SELECT COUNT(*) FROM POLICY_DOCUMENTS) AS policy_documents,
    (SELECT COUNT(*) FROM INCIDENT_REPORTS) AS incident_reports;
