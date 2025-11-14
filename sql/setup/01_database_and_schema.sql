-- ============================================================================
-- Applied Data Finance (ADF) Intelligence Agent - Database and Schema Setup
-- ============================================================================
-- Purpose: Initialize the database, schemas, and warehouse for the ADF SI solution
-- ============================================================================

-- Create the database
CREATE DATABASE IF NOT EXISTS ADF_INTELLIGENCE;

-- Use the database
USE DATABASE ADF_INTELLIGENCE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Create a virtual warehouse for query processing
CREATE OR REPLACE WAREHOUSE ADF_SI_WH WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Applied Data Finance SI workloads';

-- Set the warehouse as active
USE WAREHOUSE ADF_SI_WH;

-- Display confirmation
SELECT 'ADF database, schema, and warehouse setup completed successfully' AS STATUS;
