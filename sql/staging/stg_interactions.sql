-- Staging: interactions. Standardize types.
CREATE OR REPLACE TABLE staging.stg_interactions AS
SELECT
    id,
    case_id,
    TRIM(type) AS type,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(duration_minutes AS DOUBLE) AS duration_minutes,
    advocate_id
FROM raw.interactions;
