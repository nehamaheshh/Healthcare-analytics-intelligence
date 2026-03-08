-- Staging: case_events. Standardize types.
CREATE OR REPLACE TABLE staging.stg_case_events AS
SELECT
    id,
    case_id,
    TRIM(event_type) AS event_type,
    CAST(occurred_at AS TIMESTAMP) AS occurred_at
FROM raw.case_events;
