-- Staging: patient_feedback. Standardize types.
CREATE OR REPLACE TABLE staging.stg_feedback AS
SELECT
    id,
    case_id,
    CAST(csat_score AS DOUBLE) AS csat_score,
    TRIM(nps_bucket) AS nps_bucket,
    TRIM(feedback_text) AS feedback_text,
    CAST(submitted_at AS TIMESTAMP) AS submitted_at
FROM raw.patient_feedback;
