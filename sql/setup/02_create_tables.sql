-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - Table Definitions
-- ============================================================================
-- Purpose: Create raw tables aligned to ADF lending lifecycle (customers,
--          loan apps, underwriting, servicing, support, documents)
-- ============================================================================

USE DATABASE ADF_INTELLIGENCE;
USE SCHEMA RAW;
USE WAREHOUSE ADF_SI_WH;

-- ============================================================================
-- CUSTOMERS (applicants + existing borrowers)
-- ============================================================================
CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    phone VARCHAR(25),
    ssn_hash VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(2),
    postal_code VARCHAR(15),
    country VARCHAR(50) DEFAULT 'USA',
    employment_status VARCHAR(50),
    annual_income NUMBER(12,2),
    credit_score NUMBER(5,0),
    risk_segment VARCHAR(30),
    preferred_language VARCHAR(30),
    onboarding_channel VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- LOAN_APPLICATIONS (new loan requests)
-- ============================================================================
CREATE OR REPLACE TABLE LOAN_APPLICATIONS (
    application_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    product_type VARCHAR(50) NOT NULL,
    application_date DATE NOT NULL,
    requested_amount NUMBER(12,2) NOT NULL,
    term_months NUMBER(4,0) NOT NULL,
    purpose VARCHAR(100),
    channel VARCHAR(50),
    status VARCHAR(30) DEFAULT 'SUBMITTED',
    decision_date DATE,
    approved_amount NUMBER(12,2),
    interest_rate NUMBER(6,3),
    underwriter_id VARCHAR(20),
    decline_reason VARCHAR(200),
    risk_grade VARCHAR(10),
    automation_score NUMBER(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- LOAN_ACCOUNTS (funded/active loans)
-- ============================================================================
CREATE OR REPLACE TABLE LOAN_ACCOUNTS (
    loan_id VARCHAR(20) PRIMARY KEY,
    application_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    funding_date DATE NOT NULL,
    original_amount NUMBER(12,2) NOT NULL,
    outstanding_principal NUMBER(12,2) NOT NULL,
    accrued_interest NUMBER(12,2) DEFAULT 0,
    product_type VARCHAR(50),
    term_months NUMBER(4,0),
    interest_rate NUMBER(6,3),
    amortization_type VARCHAR(30),
    payment_due_day NUMBER(2,0),
    servicing_status VARCHAR(30) DEFAULT 'CURRENT',
    delinquency_bucket VARCHAR(20),
    next_payment_due DATE,
    last_payment_date DATE,
    last_statement_date DATE,
    auto_pay_enabled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (application_id) REFERENCES LOAN_APPLICATIONS(application_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- PAYMENT_HISTORY (loan servicing cash flows)
-- ============================================================================
CREATE OR REPLACE TABLE PAYMENT_HISTORY (
    payment_id VARCHAR(25) PRIMARY KEY,
    loan_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    payment_date DATE NOT NULL,
    posted_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    payment_method VARCHAR(30),
    payment_channel VARCHAR(30),
    amount NUMBER(12,2) NOT NULL,
    principal_component NUMBER(12,2),
    interest_component NUMBER(12,2),
    fees_component NUMBER(12,2),
    late_fee_applied BOOLEAN DEFAULT FALSE,
    nsf_flag BOOLEAN DEFAULT FALSE,
    reversal_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (loan_id) REFERENCES LOAN_ACCOUNTS(loan_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- COLLECTION_EVENTS (loss-mit and collections)
-- ============================================================================
CREATE OR REPLACE TABLE COLLECTION_EVENTS (
    collection_id VARCHAR(25) PRIMARY KEY,
    loan_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    event_type VARCHAR(50),
    agent_id VARCHAR(20),
    outcome VARCHAR(50),
    promise_to_pay_date DATE,
    promise_amount NUMBER(12,2),
    notes VARCHAR(2000),
    severity VARCHAR(20),
    next_action VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (loan_id) REFERENCES LOAN_ACCOUNTS(loan_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- CUSTOMER_INTERACTIONS (omni-channel servicing notes)
-- ============================================================================
CREATE OR REPLACE TABLE CUSTOMER_INTERACTIONS (
    interaction_id VARCHAR(25) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    loan_id VARCHAR(20),
    interaction_date TIMESTAMP_NTZ NOT NULL,
    interaction_type VARCHAR(50),
    channel VARCHAR(50),
    agent_name VARCHAR(100),
    topic VARCHAR(100),
    outcome VARCHAR(100),
    sentiment_score NUMBER(5,2),
    notes VARCHAR(5000),
    follow_up_date DATE,
    escalation_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id),
    FOREIGN KEY (loan_id) REFERENCES LOAN_ACCOUNTS(loan_id)
);

-- ============================================================================
-- SUPPORT_TRANSCRIPTS (unstructured support text)
-- ============================================================================
CREATE OR REPLACE TABLE SUPPORT_TRANSCRIPTS (
    transcript_id VARCHAR(25) PRIMARY KEY,
    interaction_id VARCHAR(25),
    customer_id VARCHAR(20),
    loan_id VARCHAR(20),
    transcript_text VARCHAR(16777216) NOT NULL,
    interaction_channel VARCHAR(50),
    transcript_date TIMESTAMP_NTZ NOT NULL,
    agent_id VARCHAR(20),
    issue_category VARCHAR(50),
    resolution_status VARCHAR(30),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (interaction_id) REFERENCES CUSTOMER_INTERACTIONS(interaction_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id),
    FOREIGN KEY (loan_id) REFERENCES LOAN_ACCOUNTS(loan_id)
);

-- ============================================================================
-- POLICY_DOCUMENTS (unstructured policies, procedures)
-- ============================================================================
CREATE OR REPLACE TABLE POLICY_DOCUMENTS (
    policy_id VARCHAR(25) PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    content VARCHAR(16777216) NOT NULL,
    document_category VARCHAR(50),
    business_unit VARCHAR(50),
    owner VARCHAR(100),
    effective_date DATE,
    revision VARCHAR(20),
    keywords VARCHAR(500),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- INCIDENT_REPORTS (risk/compliance investigations)
-- ============================================================================
CREATE OR REPLACE TABLE INCIDENT_REPORTS (
    incident_report_id VARCHAR(25) PRIMARY KEY,
    loan_id VARCHAR(20),
    customer_id VARCHAR(20),
    report_text VARCHAR(16777216) NOT NULL,
    incident_type VARCHAR(50),
    severity VARCHAR(20),
    status VARCHAR(30),
    findings_summary VARCHAR(5000),
    recommendations VARCHAR(5000),
    report_date TIMESTAMP_NTZ NOT NULL,
    investigator VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (loan_id) REFERENCES LOAN_ACCOUNTS(loan_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

-- ============================================================================
-- Display confirmation
-- ============================================================================
SELECT 'All ADF raw tables created successfully' AS STATUS;
