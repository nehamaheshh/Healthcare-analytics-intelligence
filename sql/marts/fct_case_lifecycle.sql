-- Mart: one row per case with all metrics for dashboards.
-- SLA breach: resolved > 7 days or open and created > 7 days ago with no resolution.
-- Stale: no touch in last 48 hours (using last_interaction_at or created_at).
CREATE OR REPLACE TABLE marts.fct_case_lifecycle AS
WITH base AS (
    SELECT
        t.case_id,
        t.patient_id,
        t.advocate_id,
        t.case_type,
        t.urgency,
        t.status,
        t.created_at,
        t.assigned_at,
        COALESCE(t.first_response_at_event, t.first_response_at_case) AS first_response_at,
        t.resolved_at,
        t.escalation_flag,
        t.reopen_flag,
        t.handoff_count,
        t.complexity,
        t.missing_docs,
        t.hours_to_first_response,
        t.days_to_resolution,
        t.interaction_count,
        t.last_interaction_at,
        t.avg_interaction_duration_minutes,
        f.csat_score,
        f.nps_bucket
    FROM intermediate.int_case_timings t
    LEFT JOIN staging.stg_feedback f ON f.case_id = t.case_id
),
with_flags AS (
    SELECT
        *,
        -- SLA breach: resolution took > 7 days, or still open and past 7 days since creation
        (
            (resolved_at IS NOT NULL AND days_to_resolution > 7)
            OR (resolved_at IS NULL AND DATE_DIFF('day', created_at::DATE, CURRENT_DATE) > 7)
        ) AS sla_breach_flag,
        -- Stale: no interaction in last 48 hours (for open cases)
        (
            resolved_at IS NULL
            AND (
                last_interaction_at IS NULL AND DATE_DIFF('hour', created_at, CURRENT_TIMESTAMP) > 48
                OR (last_interaction_at IS NOT NULL AND DATE_DIFF('hour', last_interaction_at, CURRENT_TIMESTAMP) > 48)
            )
        ) AS stale_case_flag
    FROM base
)
SELECT * FROM with_flags;
