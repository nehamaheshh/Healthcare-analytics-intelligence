-- Staging: cases. Parse metadata_json into structured columns and standardize types.
CREATE OR REPLACE TABLE staging.stg_cases AS
SELECT
    id,
    patient_id,
    advocate_id,
    TRIM(case_type) AS case_type,
    TRIM(urgency) AS urgency,
    TRIM(status) AS status,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(assigned_at AS TIMESTAMP) AS assigned_at,
    CASE WHEN TRIM(CAST(first_response_at AS VARCHAR)) = '' THEN NULL ELSE CAST(first_response_at AS TIMESTAMP) END AS first_response_at,
    CASE WHEN TRIM(CAST(resolved_at AS VARCHAR)) = '' THEN NULL ELSE CAST(resolved_at AS TIMESTAMP) END AS resolved_at,
    CAST(escalation_flag AS INTEGER) = 1 AS escalation_flag,
    CAST(reopen_flag AS INTEGER) = 1 AS reopen_flag,
    metadata_json,
    -- Parsed JSON fields
    COALESCE(CAST(json_extract_string(metadata_json, '$.handoff_count') AS INTEGER), 0) AS handoff_count,
    TRIM(COALESCE(json_extract_string(metadata_json, '$.complexity'), 'medium')) AS complexity,
    COALESCE(json_extract_string(metadata_json, '$.missing_docs') = 'true', false) AS missing_docs,
    TRIM(COALESCE(json_extract_string(metadata_json, '$.intake_source'), '')) AS intake_source,
    COALESCE(json_extract_string(metadata_json, '$.barriers'), '[]') AS barriers_json,
    COALESCE(json_extract_string(metadata_json, '$.support_flags'), '[]') AS support_flags_json,
    COALESCE(json_extract_string(metadata_json, '$.submitted_documents'), '[]') AS submitted_documents_json
FROM raw.cases;
