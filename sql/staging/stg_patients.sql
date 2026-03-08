-- Staging: patients. Standardize types and categories.
CREATE OR REPLACE TABLE staging.stg_patients AS
SELECT
    id,
    TRIM(insurance_type) AS insurance_type,
    TRIM(preferred_channel) AS preferred_channel,
    TRIM(state) AS state,
    CAST(risk_score AS DOUBLE) AS risk_score,
    CAST(created_at AS TIMESTAMP) AS created_at
FROM raw.patients;
