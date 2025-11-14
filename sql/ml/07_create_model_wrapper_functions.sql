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
DROP FUNCTION IF EXISTS PREDICT_PAYMENT_VOLUME(VARIANT);

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
        DATE_TRUNC('month', payment_date) AS payment_month,
        SUM(amount)::FLOAT AS total_payments
    FROM RAW.PAYMENT_HISTORY
    WHERE payment_date >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY 1
    ORDER BY payment_month
    """

    input_df = session.sql(recent_query)

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
DROP FUNCTION IF EXISTS PREDICT_BORROWER_RISK(VARIANT);

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
        c.credit_score::FLOAT AS credit_score,
        c.annual_income::FLOAT AS income,
        COUNT(DISTINCT l.loan_id) AS active_loans,
        SUM(l.outstanding_principal)::FLOAT AS outstanding_principal,
        COUNT_IF(l.servicing_status = 'DELINQUENT') AS delinquent_loans,
        AVG(ph.amount)::FLOAT AS avg_payment_amount
    FROM RAW.CUSTOMERS c
    LEFT JOIN RAW.LOAN_ACCOUNTS l ON c.customer_id = l.customer_id
    LEFT JOIN RAW.PAYMENT_HISTORY ph ON l.loan_id = ph.loan_id
    WHERE 1=1 {segment_clause}
    GROUP BY c.customer_id, c.credit_score, c.annual_income
    LIMIT 200
    """

    input_df = session.sql(query)

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
DROP FUNCTION IF EXISTS PREDICT_COLLECTION_SUCCESS(VARIANT);

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
        loans.loan_id,
        loans.delinquency_bucket,
        loans.outstanding_principal::FLOAT AS outstanding_principal,
        loans.servicing_status,
        COUNT(DISTINCT payments.payment_id) AS payment_count,
        AVG(payments.amount)::FLOAT AS avg_payment,
        COUNT_IF(collections.promise_to_pay_date IS NOT NULL) AS historical_ptp
    FROM RAW.LOAN_ACCOUNTS loans
    LEFT JOIN RAW.PAYMENT_HISTORY payments ON loans.loan_id = payments.loan_id
    LEFT JOIN RAW.COLLECTION_EVENTS collections ON loans.loan_id = collections.loan_id
    WHERE loans.servicing_status <> 'CHARGED_OFF' {bucket_clause}
    GROUP BY loans.loan_id, loans.delinquency_bucket, loans.outstanding_principal, loans.servicing_status
    LIMIT 200
    """

    input_df = session.sql(query)

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
