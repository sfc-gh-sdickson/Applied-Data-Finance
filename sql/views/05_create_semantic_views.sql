-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - Semantic Views
-- ============================================================================
-- Syntax verified against docs/create_semantic_view.html (lines 31-99, 44-80)
-- ============================================================================

USE DATABASE ADF_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ADF_SI_WH;

-- ============================================================================
-- Semantic View 1: Borrower & Loan Performance
-- ============================================================================
CREATE OR REPLACE SEMANTIC VIEW SV_BORROWER_LOAN_INTELLIGENCE
  TABLES (
    customers AS RAW.CUSTOMERS
      PRIMARY KEY (customer_id)
      WITH SYNONYMS = ('borrowers','applicants','customer accounts')
      COMMENT = 'Borrowers applying for and holding loans',
    loan_apps AS RAW.LOAN_APPLICATIONS
      PRIMARY KEY (application_id)
      WITH SYNONYMS = ('loan applications','credit requests')
      COMMENT = 'Loan application submissions',
    loans AS RAW.LOAN_ACCOUNTS
      PRIMARY KEY (loan_id)
      WITH SYNONYMS = ('loan accounts','funded loans')
      COMMENT = 'Funded loan accounts with servicing data',
    payments AS RAW.PAYMENT_HISTORY
      PRIMARY KEY (payment_id)
      WITH SYNONYMS = ('loan payments','servicing cashflows')
      COMMENT = 'Historical payment records',
    collections AS RAW.COLLECTION_EVENTS
      PRIMARY KEY (collection_id)
      WITH SYNONYMS = ('collection events','loss mitigation actions')
      COMMENT = 'Collections and loss mitigation contacts'
  )
  RELATIONSHIPS (
    loan_apps(customer_id) REFERENCES customers(customer_id),
    loans(application_id) REFERENCES loan_apps(application_id),
    loans(customer_id) REFERENCES customers(customer_id),
    payments(loan_id) REFERENCES loans(loan_id),
    payments(customer_id) REFERENCES customers(customer_id),
    collections(loan_id) REFERENCES loans(loan_id),
    collections(customer_id) REFERENCES customers(customer_id)
  )
  DIMENSIONS (
    customers.borrower_first_name AS customers.first_name
      WITH SYNONYMS = ('customer first name','applicant first name')
      COMMENT = 'Borrower given name',
    customers.borrower_last_name AS customers.last_name
      WITH SYNONYMS = ('customer last name','applicant surname')
      COMMENT = 'Borrower family name',
    customers.borrower_state AS customers.state
      WITH SYNONYMS = ('borrower state','customer state')
      COMMENT = 'State of residence',
    customers.borrower_risk_segment AS customers.risk_segment
      WITH SYNONYMS = ('risk tier','credit risk segment')
      COMMENT = 'Risk segment based on credit score',
    customers.onboarding_channel AS customers.onboarding_channel
      COMMENT = 'Acquisition channel used during onboarding',
    loan_apps.application_product_type AS loan_apps.product_type
      WITH SYNONYMS = ('application product type','requested product')
      COMMENT = 'Product requested in the application',
    loan_apps.application_status AS loan_apps.status
      WITH SYNONYMS = ('app status','decision status')
      COMMENT = 'Application status (APPROVED, DECLINED, etc.)',
    loans.loan_product_type AS loans.product_type
      WITH SYNONYMS = ('loan product','funded product type')
      COMMENT = 'Product type for the funded loan',
    loans.servicing_status AS loans.servicing_status
      WITH SYNONYMS = ('loan servicing status','portfolio status')
      COMMENT = 'Overall servicing status (CURRENT, DELINQUENT, etc.)',
    loans.delinquency_bucket AS loans.delinquency_bucket
      WITH SYNONYMS = ('dpd bucket','late bucket')
      COMMENT = 'Delinquency bucket (30/60/90+) if applicable',
    loans.autopay_flag AS loans.auto_pay_enabled
      WITH SYNONYMS = ('auto debit enabled','auto pay flag')
      COMMENT = 'Indicates whether borrower enrolled in auto-pay',
    payments.payment_method AS payments.payment_method
      WITH SYNONYMS = ('payment tender','remittance method')
      COMMENT = 'Payment method used (ACH, card, etc.)',
    payments.payment_channel AS payments.payment_channel
      WITH SYNONYMS = ('payment channel','payment source channel')
      COMMENT = 'Channel the payment came through',
    collections.collection_event_type AS collections.event_type
      WITH SYNONYMS = ('collections contact type','loss-mit contact type')
      COMMENT = 'Type of collection interaction',
    collections.collection_severity AS collections.severity
      WITH SYNONYMS = ('collections severity','loss-mit priority')
      COMMENT = 'Severity of the collection event'
  )
  METRICS (
    customers.total_borrowers AS COUNT(DISTINCT customer_id)
      WITH SYNONYMS = ('borrower count','customer count')
      COMMENT = 'Total distinct borrowers',
    loan_apps.total_applications AS COUNT(DISTINCT application_id)
      COMMENT = 'Total loan applications submitted',
    loan_apps.approval_rate AS (COUNT_IF(status = 'APPROVED')::FLOAT / NULLIF(COUNT(*),0))
      WITH SYNONYMS = ('approval rate','app conversion rate')
      COMMENT = 'Ratio of approved applications',
    loans.total_funded_loans AS COUNT(DISTINCT loan_id)
      WITH SYNONYMS = ('funded loans','loan portfolio count')
      COMMENT = 'Number of funded loan accounts',
    loans.total_original_principal AS SUM(original_amount)
      WITH SYNONYMS = ('original principal sum','funded principal')
      COMMENT = 'Sum of original principal funded',
    loans.total_outstanding_principal AS SUM(outstanding_principal)
      WITH SYNONYMS = ('outstanding balance','principal outstanding')
      COMMENT = 'Current principal outstanding across loans',
    loans.delinquent_loans AS COUNT_IF(servicing_status = 'DELINQUENT')
      WITH SYNONYMS = ('delinquent loan count','dpd count')
      COMMENT = 'Number of loans currently delinquent',
    payments.total_payments AS COUNT(DISTINCT payment_id)
      WITH SYNONYMS = ('payment count','servicing payment count')
      COMMENT = 'Number of payments posted',
    payments.total_payment_amount AS SUM(amount)
      WITH SYNONYMS = ('payments collected','cash collected')
      COMMENT = 'Total payment dollars collected',
    payments.avg_payment_amount AS AVG(amount)
      WITH SYNONYMS = ('avg payment amt','mean remittance')
      COMMENT = 'Average payment size',
    collections.total_collection_events AS COUNT(DISTINCT collection_id)
      WITH SYNONYMS = ('collections count','loss-mit events')
      COMMENT = 'Number of collection interactions',
    collections.promise_to_pay_rate AS (COUNT_IF(promise_to_pay_date IS NOT NULL)::FLOAT / NULLIF(COUNT(*),0))
      WITH SYNONYMS = ('promise to pay rate','ptp rate')
      COMMENT = 'Share of events resulting in a promise to pay'
  )
  COMMENT = 'Semantic view for borrower lifecycle and loan performance across onboarding, funding, payments, and collections';

-- ============================================================================
-- Semantic View 2: Servicing Risk & Collections Insight
-- ============================================================================
CREATE OR REPLACE SEMANTIC VIEW SV_SERVICING_COLLECTIONS_INTELLIGENCE
  TABLES (
    loans AS RAW.LOAN_ACCOUNTS
      PRIMARY KEY (loan_id)
      WITH SYNONYMS = ('serviced loans','portfolio loans')
      COMMENT = 'Loan accounts under servicing',
    payments AS RAW.PAYMENT_HISTORY
      PRIMARY KEY (payment_id)
      COMMENT = 'Detailed payment transactions',
    collections AS RAW.COLLECTION_EVENTS
      PRIMARY KEY (collection_id)
      COMMENT = 'Collections / workout touchpoints',
    incidents AS RAW.INCIDENT_REPORTS
      PRIMARY KEY (incident_report_id)
      COMMENT = 'Risk, QA, and compliance incidents'
  )
  RELATIONSHIPS (
    payments(loan_id) REFERENCES loans(loan_id),
    collections(loan_id) REFERENCES loans(loan_id),
    incidents(loan_id) REFERENCES loans(loan_id)
  )
  DIMENSIONS (
    loans.product_type AS loans.product_type
      COMMENT = 'Loan product category',
    loans.servicing_status AS loans.servicing_status
      COMMENT = 'Servicing state (current, delinquent, charged-off)',
    loans.delinquency_bucket AS loans.delinquency_bucket
      COMMENT = 'DPD bucket for delinquent loans',
    collections.collection_event_type AS collections.event_type
      COMMENT = 'Collection touchpoint type',
    collections.collection_outcome AS collections.outcome
      COMMENT = 'Result of contact (promise, no contact, etc.)',
    collections.severity AS collections.severity
      COMMENT = 'Priority/impact of collection event',
    incidents.incident_type AS incidents.incident_type
      COMMENT = 'Incident classification (servicing, compliance, etc.)',
    incidents.incident_status AS incidents.status
      COMMENT = 'Incident lifecycle status'
  )
  METRICS (
    loans.total_loans AS COUNT(DISTINCT loan_id)
      COMMENT = 'Total loans monitored',
    loans.total_outstanding AS SUM(outstanding_principal)
      COMMENT = 'Outstanding principal sum',
    loans.delinquent_balance AS SUM(CASE WHEN servicing_status = 'DELINQUENT' THEN outstanding_principal ELSE 0 END)
      COMMENT = 'Outstanding principal on delinquent loans',
    payments.total_payments AS COUNT(DISTINCT payment_id)
      COMMENT = 'Total number of payments posted',
    payments.total_collected AS SUM(amount)
      COMMENT = 'Total dollars collected via payments',
    payments.late_payment_rate AS (COUNT_IF(late_fee_applied)::FLOAT / NULLIF(COUNT(*),0))
      COMMENT = 'Share of payments assessed late fees',
    collections.collection_touchpoints AS COUNT(DISTINCT collection_id)
      COMMENT = 'Number of collection contacts',
    collections.ptp_conversion AS (COUNT_IF(outcome = 'PROMISE_TO_PAY')::FLOAT / NULLIF(COUNT(*),0))
      COMMENT = 'Promise-to-pay conversion rate',
    incidents.total_incidents AS COUNT(DISTINCT incident_report_id)
      COMMENT = 'Number of incidents recorded',
    incidents.open_incidents AS COUNT_IF(status <> 'CLOSED')
      COMMENT = 'Open incident count'
  )
  COMMENT = 'Semantic view focused on servicing health, collections effectiveness, and incident tracking';

-- ============================================================================
-- Semantic View 3: Customer Experience & Support Intelligence
-- ============================================================================
CREATE OR REPLACE SEMANTIC VIEW SV_CUSTOMER_SUPPORT_INTELLIGENCE
  TABLES (
    customers AS RAW.CUSTOMERS
      PRIMARY KEY (customer_id)
      COMMENT = 'Borrowers interacting with support',
    interactions AS RAW.CUSTOMER_INTERACTIONS
      PRIMARY KEY (interaction_id)
      COMMENT = 'Structured support/servicing interactions',
    transcripts AS RAW.SUPPORT_TRANSCRIPTS
      PRIMARY KEY (transcript_id)
      COMMENT = 'Unstructured support conversation transcripts',
    incidents AS RAW.INCIDENT_REPORTS
      PRIMARY KEY (incident_report_id)
      COMMENT = 'Issues raised through support channels'
  )
  RELATIONSHIPS (
    interactions(customer_id) REFERENCES customers(customer_id),
    transcripts(interaction_id) REFERENCES interactions(interaction_id),
    incidents(customer_id) REFERENCES customers(customer_id)
  )
  DIMENSIONS (
    customers.customer_state AS customers.state
      COMMENT = 'Borrower state',
    customers.risk_segment AS customers.risk_segment
      COMMENT = 'Borrower risk tier',
    interactions.interaction_type AS interactions.interaction_type
      COMMENT = 'Support topic classification',
    interactions.interaction_channel AS interactions.channel
      COMMENT = 'Channel used in interaction',
    interactions.interaction_outcome AS interactions.outcome
      COMMENT = 'Result of support touchpoint',
    interactions.escalated AS interactions.escalation_flag
      COMMENT = 'Indicates whether interaction escalated',
    transcripts.transcript_channel AS transcripts.interaction_channel
      COMMENT = 'Channel recorded in transcript',
    transcripts.issue_category AS transcripts.issue_category
      COMMENT = 'Issue category derived from transcript',
    transcripts.resolution_status AS transcripts.resolution_status
      COMMENT = 'Transcript resolution status',
    incidents.incident_type AS incidents.incident_type
      COMMENT = 'Support-related incident type'
  )
  METRICS (
    customers.total_customers AS COUNT(DISTINCT customer_id)
      COMMENT = 'Customers with support activity',
    interactions.total_interactions AS COUNT(DISTINCT interaction_id)
      COMMENT = 'Total support/servicing interactions',
    interactions.escalation_rate AS (COUNT_IF(escalation_flag)::FLOAT / NULLIF(COUNT(*),0))
      COMMENT = 'Percentage of interactions escalated',
    interactions.avg_sentiment AS AVG(sentiment_score)
      COMMENT = 'Average sentiment score from interactions',
    transcripts.total_transcripts AS COUNT(DISTINCT transcript_id)
      COMMENT = 'Number of transcripts ingested',
    transcripts.resolution_rate AS (COUNT_IF(resolution_status = 'RESOLVED')::FLOAT / NULLIF(COUNT(*),0))
      COMMENT = 'Share of transcripts marked resolved',
    incidents.total_support_incidents AS COUNT(DISTINCT incident_report_id)
      COMMENT = 'Support-originated incident volume'
  )
  COMMENT = 'Semantic view linking customer profiles with structured interactions, transcripts, and incidents for support intelligence';

-- ============================================================================
-- Confirmation
-- ============================================================================
SELECT 'ADF semantic views created successfully' AS STATUS;
