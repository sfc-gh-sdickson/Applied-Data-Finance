<img src="Snowflake_Logo.svg" alt="Snowflake Logo" width="33%">

# Applied Data Finance Question Catalog

Each prompt below is validated against the synthetic data model delivered in this project and can be answered by the `ADF_INTELLIGENCE_AGENT` (structured, search, or ML tools as noted).

## Simple Questions (Direct Metrics)
1. How many active borrowers currently have auto-pay enabled, and what percent of total borrowers does that represent?
2. What is the total outstanding principal by loan product type right now?
3. How many hardship-related support interactions were escalated in the past 30 days?
4. Which borrower state has the highest number of open loans, and what is the average credit score there?
5. What is the average sentiment score for payoff-quote interactions this month?

## Complex Questions (Multi-step SQL / Search)
1. Compare delinquency rates by risk segment and loan product—identify the riskiest combination and its outstanding balance.
2. Show monthly payment volume trends alongside the count of severe collection events for the past year.
3. List the top five borrowers by outstanding principal who have multiple open promise-to-pay commitments and no payments in the last 45 days.
4. Correlate support escalation frequency with subsequent delinquency buckets for each borrower risk segment.
5. Summarize compliance incidents by severity and tie them to the originating support topics.

## ML-Driven Questions (Model Tools)
1. Predict payment volume for the next 6 months and highlight any months that exceed our current cash-flow threshold.
2. Using the Borrower Risk model, list near-prime borrowers with delinquency probability ≥ 0.6 and total outstanding principal above $15,000.
3. Estimate promise-to-pay success odds for 60-day delinquent loans with outstanding principal above $5,000 and prioritize the top 10.
4. Assuming active loans grow 5% month-over-month, rerun the Payment Volume forecaster and visualize the updated trajectory.
5. Compare model-predicted promise-to-pay success versus the actual historical success rate for each delinquency bucket to calibrate collections.
