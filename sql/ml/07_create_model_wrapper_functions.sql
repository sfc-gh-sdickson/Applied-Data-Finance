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
DROP PROCEDURE IF EXISTS PREDICT_PAYMENT_VOLUME(INT);

CREATE OR REPLACE PROCEDURE PREDICT_PAYMENT_VOLUME(
    MONTHS_AHEAD INT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'pandas')
HANDLER = 'predict_payments'
COMMENT = 'Invokes PAYMENT_VOLUME_FORECASTER model to project future payment cash flows'
AS
$$
import json
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
from snowflake.ml.registry import Registry

def predict_payments(session, months_ahead: int):
    reg = Registry(session)
    model = reg.get_model("PAYMENT_VOLUME_FORECASTER").default

    # Query for the last available month of data to use as a baseline
    base_query = """
    SELECT
        DATE_TRUNC('month', payment_date)::DATE AS payment_month,
        COUNT(DISTINCT loan_id)::FLOAT AS loan_count,
        AVG(amount)::FLOAT AS avg_payment_amount,
        COUNT_IF(late_fee_applied)::FLOAT AS late_payment_count
    FROM RAW.PAYMENT_HISTORY
    ORDER BY payment_month DESC
    LIMIT 1
    """
    base_df = session.sql(base_query).to_pandas()
    base_date = pd.to_datetime(base_df['PAYMENT_MONTH'].iloc[0])
    
    # Generate future dates and features for prediction
    future_dates = [base_date + relativedelta(months=i) for i in range(1, months_ahead + 1)]
    future_features_list = []
    for dt in future_dates:
        future_features_list.append({
            "MONTH_NUM": dt.month,
            "YEAR_NUM": dt.year,
            "LOAN_COUNT": base_df['LOAN_COUNT'].iloc[0],
            "AVG_PAYMENT_AMOUNT": base_df['AVG_PAYMENT_AMOUNT'].iloc[0],
            "LATE_PAYMENT_COUNT": base_df['LATE_PAYMENT_COUNT'].iloc[0]
        })

    input_df = session.create_dataframe(pd.DataFrame(future_features_list))
    
    # Make predictions
    preds = model.run(input_df, function_name="predict")
    
    # Combine predictions with future dates for clarity
    preds_pdf = preds.to_pandas()
    results_df = pd.DataFrame({
        "FORECAST_MONTH": [d.strftime('%Y-%m') for d in future_dates],
        "PREDICTED_PAYMENT_AMOUNT": preds_pdf["PREDICTED_PAYMENT_AMOUNT"]
    })

    return json.dumps({
        "months_ahead": months_ahead,
        "prediction": results_df.to_dict(orient="records")
    })
$$;

-- ============================================================================
-- Procedure 2: Borrower Churn / Default Risk Wrapper
-- ============================================================================
DROP PROCEDURE IF EXISTS PREDICT_BORROWER_RISK(VARCHAR);

CREATE OR REPLACE PROCEDURE PREDICT_BORROWER_RISK(
    RISK_SEGMENT_FILTER VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'pandas')
HANDLER = 'predict_risk'
COMMENT = 'Invokes BORROWER_RISK_MODEL to assess churn/delinquency risk by segment'
AS
$$
def predict_risk(session, risk_segment_filter):
    from snowflake.ml.registry import Registry
    import json

    reg = Registry(session)
    model = reg.get_model("BORROWER_RISK_MODEL").default

    segment_clause = f"AND c.risk_segment = '{risk_segment_filter}'" if risk_segment_filter and risk_segment_filter.upper() != 'ALL' else ""

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
DROP PROCEDURE IF EXISTS PREDICT_COLLECTION_SUCCESS(VARCHAR);

CREATE OR REPLACE PROCEDURE PREDICT_COLLECTION_SUCCESS(
    DELINQUENCY_BUCKET_FILTER VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-ml-python', 'pandas')
HANDLER = 'predict_collections'
COMMENT = 'Invokes COLLECTION_SUCCESS_MODEL to estimate promise-to-pay success probability'
AS
$$
def predict_collections(session, delinquency_bucket_filter):
    from snowflake.ml.registry import Registry
    import json

    reg = Registry(session)
    model = reg.get_model("COLLECTION_SUCCESS_MODEL").default

    bucket_clause = f"AND loans.delinquency_bucket = '{delinquency_bucket_filter}'" if delinquency_bucket_filter and delinquency_bucket_filter.upper() != 'ALL' else ""

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
