-- Mart: aggregate KPIs by case_type and overall for executive dashboard.
CREATE OR REPLACE TABLE marts.mart_ops_kpis AS
SELECT
    case_type,
    COUNT(*) AS total_cases,
    COUNT(*) FILTER (WHERE status NOT IN ('resolved', 'reopened')) AS active_cases,
    COUNT(*) FILTER (WHERE status NOT IN ('resolved', 'reopened')) AS open_backlog,
    ROUND(AVG(hours_to_first_response) FILTER (WHERE hours_to_first_response IS NOT NULL), 2) AS avg_hours_to_first_response,
    ROUND(AVG(days_to_resolution) FILTER (WHERE days_to_resolution IS NOT NULL), 2) AS avg_days_to_resolution,
    ROUND(100.0 * COUNT(*) FILTER (WHERE escalation_flag) / NULLIF(COUNT(*), 0), 2) AS escalation_rate_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE reopen_flag) / NULLIF(COUNT(*) FILTER (WHERE status IN ('resolved', 'reopened')), 0), 2) AS reopen_rate_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE sla_breach_flag) / NULLIF(COUNT(*), 0), 2) AS sla_breach_rate_pct,
    ROUND(AVG(interaction_count), 2) AS avg_interactions_per_case,
    ROUND(AVG(csat_score) FILTER (WHERE csat_score IS NOT NULL), 2) AS avg_csat,
    COUNT(*) FILTER (WHERE stale_case_flag) AS stale_cases_count
FROM marts.fct_case_lifecycle
GROUP BY case_type;
