-- Intermediate: case-level timings and interaction metrics.
CREATE OR REPLACE TABLE intermediate.int_case_timings AS
WITH first_response AS (
    SELECT
        case_id,
        MIN(occurred_at) AS first_response_at
    FROM staging.stg_case_events
    WHERE event_type = 'first_response'
    GROUP BY case_id
),
interaction_stats AS (
    SELECT
        case_id,
        COUNT(*) AS interaction_count,
        MAX(created_at) AS last_interaction_at,
        AVG(duration_minutes) AS avg_duration_minutes
    FROM staging.stg_interactions
    GROUP BY case_id
)
SELECT
    c.id AS case_id,
    c.patient_id,
    c.advocate_id,
    c.case_type,
    c.urgency,
    c.status,
    c.created_at,
    c.assigned_at,
    c.first_response_at AS first_response_at_case,
    fr.first_response_at AS first_response_at_event,
    c.resolved_at,
    c.escalation_flag,
    c.reopen_flag,
    c.handoff_count,
    c.complexity,
    c.missing_docs,
    EXTRACT(EPOCH FROM (COALESCE(fr.first_response_at, c.first_response_at) - c.created_at)) / 3600.0 AS hours_to_first_response,
    CASE
        WHEN c.resolved_at IS NOT NULL THEN DATE_DIFF('day', c.created_at::DATE, c.resolved_at::DATE)
        ELSE NULL
    END AS days_to_resolution,
    COALESCE(i.interaction_count, 0) AS interaction_count,
    i.last_interaction_at AS last_interaction_at,
    i.avg_duration_minutes AS avg_interaction_duration_minutes
FROM staging.stg_cases c
LEFT JOIN first_response fr ON fr.case_id = c.id
LEFT JOIN interaction_stats i ON i.case_id = c.id;
