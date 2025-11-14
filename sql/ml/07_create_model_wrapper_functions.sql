-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - ML Wrapper Procedures
-- ============================================================================
-- Syntax mirrors Axon Demo/sql/ml/07_create_model_wrapper_functions.sql.
-- ============================================================================

USE DATABASE ADF_INTELLIGENCE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE ADF_SI_WH;

-- ============================================================================
-- Procedure 1: Payment Volume Forecast Wrapper
-- ============================================================================
DROP PROCEDURE IF EXISTS PREDICT_PAYMENT_VOLUME(VARIANT);

CREATE OR REPLACE PROCEDURE PREDICT_PAYMENT_VOLUME(
    INPUT_DATA VARIANT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'pandas')
HANDLER = 'predict_payments'
COMMENT = 'Invokes PAYMENT_VOLUME_FORECASTER model to project future payment cash flows'
AS
$$
def predict_payments(session, input_data):
    from snowflake.ml.registry import Registry
    import json

    payload = {}
    months_ahead = 6

    if input_data is not None:
        if isinstance(input_data, dict):
            payload = input_data
        else:
            try:
                payload = json.loads(str(input_data))
            except Exception:
                payload = {}

    months_ahead = payload.get("months_ahead") or payload.get("monthsAhead") or months_ahead
    try:
        months_ahead = int(months_ahead)
    except Exception:
        months_ahead = 6

    reg = Registry(session)
    model = reg.get_model("PAYMENT_VOLUME_FORECASTER").default

    recent_query = f"""
    SELECT
        DATE_TRUNC('month', payment_date)::DATE AS payment_month,
        MONTH(payment_date) AS month_num,
        YEAR(payment_date) AS year_num,
        COUNT(DISTINCT loan_id)::FLOAT AS loan_count,
        AVG(amount)::FLOAT AS avg_payment_amount,
        COUNT_IF(late_fee_applied)::FLOAT AS late_payment_count,
        SUM(amount)::FLOAT AS total_payment_amount
    FROM RAW.PAYMENT_HISTORY
    WHERE payment_date >= DATEADD('month', -24, CURRENT_DATE())
    GROUP BY 1,2,3
    ORDER BY payment_month
    """

    input_df = session.sql(recent_query).drop("PAYMENT_MONTH")

    preds = model.run(input_df, function_name="predict")
    pdf = preds.to_pandas()

    return json.dumps({
        "months_ahead": months_ahead,
        "prediction": pdf.to_dict(orient="records")
    })
$$;

-- ============================================================================
-- Procedure 2: Borrower Churn / Default Risk Wrapper
-- ============================================================================
DROP PROCEDURE IF EXISTS PREDICT_BORROWER_RISK(VARIANT);

CREATE OR REPLACE PROCEDURE PREDICT_BORROWER_RISK(
    INPUT_DATA VARIANT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'pandas')
HANDLER = 'predict_risk'
COMMENT = 'Invokes BORROWER_RISK_MODEL to assess churn/delinquency risk by segment'
AS
$$
def predict_risk(session, input_data):
    from snowflake.ml.registry import Registry
    import json

    payload = {}
    risk_segment_filter = None

    if input_data is not None:
        if isinstance(input_data, dict):
            payload = input_data
        else:
            try:
                payload = json.loads(str(input_data))
            except Exception:
                payload = {}

    risk_segment_filter = payload.get("risk_segment") or payload.get("riskSegment")
    if not risk_segment_filter and isinstance(input_data, str):
        risk_segment_filter = input_data

    reg = Registry(session)
    model = reg.get_model("BORROWER_RISK_MODEL").default

    segment_clause = f"AND c.risk_segment = '{risk_segment_filter}'" if risk_segment_filter else ""

    query = f"""
    SELECT
        c.customer_id,
        c.risk_segment AS borrower_segment,
        c.employment_status,
        c.annual_income::FLOAT AS annual_income,
        c.credit_score::FLOAT AS credit_score,
        COUNT(DISTINCT l.loan_id)::FLOAT AS total_loans,
        SUM(l.outstanding_principal)::FLOAT AS outstanding_principal,
        COUNT_IF(l.servicing_status = 'DELINQUENT')::FLOAT AS delinquent_loans,
        AVG(ph.amount)::FLOAT AS avg_payment_amount,
        COUNT_IF(ph.nsf_flag)::FLOAT AS nsf_events,
        (COUNT_IF(l.servicing_status = 'DELINQUENT') > 0)::BOOLEAN AS is_delinquent
    FROM RAW.CUSTOMERS c
    LEFT JOIN RAW.LOAN_ACCOUNTS l ON c.customer_id = l.customer_id
    LEFT JOIN RAW.PAYMENT_HISTORY ph ON l.loan_id = ph.loan_id
    WHERE 1=1 {segment_clause}
    GROUP BY 1,2,3,4,5
    LIMIT 500
    """

    input_df = session.sql(query).drop("CUSTOMER_ID")

    preds = model.run(input_df, function_name="predict")
    pdf = preds.to_pandas()

    return json.dumps({
        "risk_segment": risk_segment_filter or "ALL",
        "results": pdf.to_dict(orient="records")
    })
$$;

-- ============================================================================
-- Procedure 3: Collections Outcome Prediction Wrapper
-- ============================================================================
DROP PROCEDURE IF EXISTS PREDICT_COLLECTION_SUCCESS(VARIANT);

CREATE OR REPLACE PROCEDURE PREDICT_COLLECTION_SUCCESS(
    INPUT_DATA VARIANT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'pandas')
HANDLER = 'predict_collections'
COMMENT = 'Invokes COLLECTION_SUCCESS_MODEL to estimate promise-to-pay success probability'
AS
$$
def predict_collections(session, input_data):
    from snowflake.ml.registry import Registry
    import json

    payload = {}
    delinquency_bucket_filter = None

    if input_data is not None:
        if isinstance(input_data, dict):
            payload = input_data
        else:
            try:
                payload = json.loads(str(input_data))
            except Exception:
                payload = {}

    delinquency_bucket_filter = payload.get("delinquency_bucket") or payload.get("delinquencyBucket")
    if not delinquency_bucket_filter and isinstance(input_data, str):
        delinquency_bucket_filter = input_data

    reg = Registry(session)
    model = reg.get_model("COLLECTION_SUCCESS_MODEL").default

    bucket_clause = f"AND loans.delinquency_bucket = '{delinquency_bucket_filter}'" if delinquency_bucket_filter else ""

    query = f"""
    SELECT
        ce.collection_id,
        ce.event_type,
        ce.severity,
        ce.outcome,
        COALESCE(ce.promise_amount, 0)::FLOAT AS promise_amount,
        DATEDIFF('day', ce.event_timestamp::DATE, CURRENT_DATE()) AS event_age_days,
        loans.delinquency_bucket,
        loans.servicing_status,
        loans.outstanding_principal::FLOAT AS outstanding_principal,
        (ce.promise_to_pay_date IS NOT NULL OR ce.outcome = 'PROMISE_TO_PAY')::BOOLEAN AS ptp_success
    FROM RAW.COLLECTION_EVENTS ce
    JOIN RAW.LOAN_ACCOUNTS loans ON ce.loan_id = loans.loan_id
    WHERE ce.event_timestamp >= DATEADD('month', -18, CURRENT_TIMESTAMP())
      {bucket_clause}
    LIMIT 500
    """

    input_df = session.sql(query).drop("COLLECTION_ID")

    preds = model.run(input_df, function_name="predict")
    pdf = preds.to_pandas()

    return json.dumps({
        "delinquency_bucket": delinquency_bucket_filter or "ALL",
        "results": pdf.to_dict(orient="records")
    })
$$;

-- ============================================================================
-- Confirmation
-- ============================================================================
SELECT 'ADF ML wrapper procedures created successfully' AS STATUS;
