-- Staging: advocates. Standardize types.
CREATE OR REPLACE TABLE staging.stg_advocates AS
SELECT
    id,
    TRIM(name) AS name,
    TRIM(specialty) AS specialty,
    TRIM(team) AS team,
    CAST(capacity AS INTEGER) AS capacity,
    CAST(hire_date AS DATE) AS hire_date
FROM raw.advocates;
