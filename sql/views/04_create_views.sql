-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - Analytical Views
-- ============================================================================
-- Purpose: Curated views used by semantic layer & dashboards
-- ============================================================================

USE DATABASE ADF_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ADF_SI_WH;

-- ============================================================================
-- Customer 360 View
-- ============================================================================
CREATE OR REPLACE VIEW V_CUSTOMER_360 AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.postal_code,
    c.country,
    c.employment_status,
    c.annual_income,
    c.credit_score,
    c.risk_segment,
    c.onboarding_channel,
    COUNT(DISTINCT la.application_id) AS total_applications,
    COUNT(DISTINCT CASE WHEN la.status = 'APPROVED' THEN la.application_id END) AS approved_applications,
    COUNT(DISTINCT loan.loan_id) AS active_loans,
    SUM(loan.outstanding_principal) AS total_principal_outstanding,
    SUM(ph.amount) AS total_payments_made,
    SUM(ph.fees_component) AS total_fees_paid,
    MAX(loan.servicing_status) AS latest_servicing_status,
    COUNT(DISTINCT CASE WHEN loan.servicing_status = 'DELINQUENT' THEN loan.loan_id END) AS delinquent_loans,
    COUNT(DISTINCT ce.collection_id) AS collection_contacts,
    COUNT(DISTINCT ci.interaction_id) AS service_interactions,
    AVG(ci.sentiment_score) AS avg_sentiment_score,
    c.created_at,
    c.updated_at
FROM RAW.CUSTOMERS c
LEFT JOIN RAW.LOAN_APPLICATIONS la ON c.customer_id = la.customer_id
LEFT JOIN RAW.LOAN_ACCOUNTS loan ON c.customer_id = loan.customer_id
LEFT JOIN RAW.PAYMENT_HISTORY ph ON loan.loan_id = ph.loan_id
LEFT JOIN RAW.COLLECTION_EVENTS ce ON loan.loan_id = ce.loan_id
LEFT JOIN RAW.CUSTOMER_INTERACTIONS ci ON c.customer_id = ci.customer_id
GROUP BY
    c.customer_id, c.first_name, c.last_name, c.email, c.phone,
    c.city, c.state, c.postal_code, c.country, c.employment_status,
    c.annual_income, c.credit_score, c.risk_segment, c.onboarding_channel,
    c.created_at, c.updated_at;

-- ============================================================================
-- Loan Application Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_LOAN_APPLICATION_ANALYTICS AS
SELECT
    la.application_id,
    la.customer_id,
    c.first_name,
    c.last_name,
    la.product_type,
    la.application_date,
    la.requested_amount,
    la.term_months,
    la.channel,
    la.status,
    la.decision_date,
    la.approved_amount,
    la.interest_rate,
    la.risk_grade,
    la.automation_score,
    DATEDIFF('day', la.application_date, la.decision_date) AS decision_days,
    CASE 
        WHEN la.status = 'APPROVED' THEN 'FUNNEL:APPROVED'
        WHEN la.status = 'DECLINED' THEN 'FUNNEL:DECLINED'
        ELSE 'FUNNEL:PENDING'
    END AS funnel_stage,
    la.created_at,
    la.updated_at
FROM RAW.LOAN_APPLICATIONS la
JOIN RAW.CUSTOMERS c ON la.customer_id = c.customer_id;

-- ============================================================================
-- Loan Account Performance View
-- ============================================================================
CREATE OR REPLACE VIEW V_LOAN_ACCOUNT_PERFORMANCE AS
SELECT
    loan.loan_id,
    loan.customer_id,
    loan.product_type,
    loan.funding_date,
    loan.original_amount,
    loan.outstanding_principal,
    loan.accrued_interest,
    loan.term_months,
    loan.interest_rate,
    loan.servicing_status,
    loan.delinquency_bucket,
    loan.next_payment_due,
    loan.last_payment_date,
    loan.auto_pay_enabled,
    COUNT(DISTINCT ph.payment_id) AS payment_count,
    SUM(ph.amount) AS total_paid_amount,
    SUM(ph.principal_component) AS total_principal_paid,
    SUM(ph.interest_component) AS total_interest_paid,
    AVG(ph.amount) AS avg_payment_amount,
    MAX(ph.payment_date) AS latest_payment_date,
    COUNT(DISTINCT CASE WHEN ph.late_fee_applied THEN ph.payment_id END) AS late_payments,
    COUNT(DISTINCT ce.collection_id) AS collection_contacts,
    COUNT(DISTINCT CASE WHEN ce.severity = 'HIGH' THEN ce.collection_id END) AS severe_collection_events,
    loan.created_at,
    loan.updated_at
FROM RAW.LOAN_ACCOUNTS loan
LEFT JOIN RAW.PAYMENT_HISTORY ph ON loan.loan_id = ph.loan_id
LEFT JOIN RAW.COLLECTION_EVENTS ce ON loan.loan_id = ce.loan_id
GROUP BY
    loan.loan_id, loan.customer_id, loan.product_type, loan.funding_date,
    loan.original_amount, loan.outstanding_principal, loan.accrued_interest,
    loan.term_months, loan.interest_rate, loan.servicing_status,
    loan.delinquency_bucket, loan.next_payment_due, loan.last_payment_date,
    loan.auto_pay_enabled, loan.created_at, loan.updated_at;

-- ============================================================================
-- Payment Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_PAYMENT_ANALYTICS AS
SELECT
    ph.payment_id,
    ph.loan_id,
    ph.customer_id,
    ph.payment_date,
    DATE_TRUNC('month', ph.payment_date) AS payment_month,
    ph.payment_method,
    ph.payment_channel,
    ph.amount,
    ph.principal_component,
    ph.interest_component,
    ph.fees_component,
    ph.late_fee_applied,
    ph.nsf_flag,
    loan.product_type,
    loan.servicing_status,
    loan.delinquency_bucket
FROM RAW.PAYMENT_HISTORY ph
JOIN RAW.LOAN_ACCOUNTS loan ON ph.loan_id = loan.loan_id;

-- ============================================================================
-- Collections Analytics View
-- ============================================================================
CREATE OR REPLACE VIEW V_COLLECTION_ANALYTICS AS
SELECT
    ce.collection_id,
    ce.loan_id,
    ce.customer_id,
    ce.event_timestamp,
    ce.event_type,
    ce.agent_id,
    ce.outcome,
    ce.promise_to_pay_date,
    ce.promise_amount,
    ce.severity,
    ce.next_action,
    loan.product_type,
    loan.servicing_status,
    loan.delinquency_bucket,
    c.state AS customer_state
FROM RAW.COLLECTION_EVENTS ce
JOIN RAW.LOAN_ACCOUNTS loan ON ce.loan_id = loan.loan_id
JOIN RAW.CUSTOMERS c ON ce.customer_id = c.customer_id;

-- ============================================================================
-- Customer Interaction View
-- ============================================================================
CREATE OR REPLACE VIEW V_CUSTOMER_INTERACTION_ANALYTICS AS
SELECT
    ci.interaction_id,
    ci.customer_id,
    ci.loan_id,
    ci.interaction_date,
    DATE_TRUNC('month', ci.interaction_date) AS interaction_month,
    ci.interaction_type,
    ci.channel,
    ci.agent_name,
    ci.topic,
    ci.outcome,
    ci.sentiment_score,
    ci.escalation_flag,
    ci.follow_up_date,
    loan.servicing_status,
    loan.delinquency_bucket
FROM RAW.CUSTOMER_INTERACTIONS ci
LEFT JOIN RAW.LOAN_ACCOUNTS loan ON ci.loan_id = loan.loan_id;

-- ============================================================================
-- Support Transcript View
-- ============================================================================
CREATE OR REPLACE VIEW V_SUPPORT_TRANSCRIPTS AS
SELECT
    st.transcript_id,
    st.interaction_id,
    st.customer_id,
    st.loan_id,
    st.transcript_date,
    st.interaction_channel,
    st.issue_category,
    st.resolution_status,
    st.transcript_text,
    ci.interaction_type,
    ci.topic,
    ci.outcome
FROM RAW.SUPPORT_TRANSCRIPTS st
LEFT JOIN RAW.CUSTOMER_INTERACTIONS ci ON st.interaction_id = ci.interaction_id;

-- ============================================================================
-- Incident / Risk View
-- ============================================================================
CREATE OR REPLACE VIEW V_INCIDENT_ANALYTICS AS
SELECT
    ir.incident_report_id,
    ir.loan_id,
    ir.customer_id,
    ir.incident_type,
    ir.severity,
    ir.status,
    ir.report_date,
    ir.investigator,
    ir.report_text,
    ir.findings_summary,
    ir.recommendations,
    loan.product_type,
    loan.servicing_status,
    c.state AS customer_state
FROM RAW.INCIDENT_REPORTS ir
LEFT JOIN RAW.LOAN_ACCOUNTS loan ON ir.loan_id = loan.loan_id
LEFT JOIN RAW.CUSTOMERS c ON ir.customer_id = c.customer_id;

-- ============================================================================
-- Summary View for Business KPIs
-- ============================================================================
CREATE OR REPLACE VIEW V_ADF_REVENUE_SUMMARY AS
SELECT
    loan.loan_id,
    loan.customer_id,
    loan.product_type,
    loan.funding_date,
    DATE_TRUNC('month', loan.funding_date) AS funding_month,
    loan.original_amount,
    loan.interest_rate,
    loan.servicing_status,
    SUM(ph.interest_component) AS lifetime_interest_collected,
    SUM(ph.fees_component) AS lifetime_fees_collected,
    SUM(ph.amount) AS lifetime_payments,
    MAX(ph.payment_date) AS last_payment_date,
    COUNT(DISTINCT ce.collection_id) AS collection_touchpoints,
    COUNT(DISTINCT ir.incident_report_id) AS incident_count
FROM RAW.LOAN_ACCOUNTS loan
LEFT JOIN RAW.PAYMENT_HISTORY ph ON loan.loan_id = ph.loan_id
LEFT JOIN RAW.COLLECTION_EVENTS ce ON loan.loan_id = ce.loan_id
LEFT JOIN RAW.INCIDENT_REPORTS ir ON loan.loan_id = ir.loan_id
GROUP BY
    loan.loan_id, loan.customer_id, loan.product_type, loan.funding_date,
    loan.original_amount, loan.interest_rate, loan.servicing_status;

-- ============================================================================
-- Confirmation
-- ============================================================================
SELECT 'ADF analytical views created successfully' AS STATUS;
