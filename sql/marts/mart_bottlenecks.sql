-- Bottleneck analysis: consistent schema (analysis_type, segment, case_count, avg_days_to_resolution, avg_csat, sla_breach_pct).
CREATE OR REPLACE TABLE marts.mart_bottlenecks AS
SELECT
    'missing_docs_vs_resolution' AS analysis_type,
    CASE WHEN missing_docs THEN 'missing_docs' ELSE 'docs_complete' END AS segment,
    COUNT(*) AS case_count,
    ROUND(AVG(days_to_resolution), 2) AS avg_days_to_resolution,
    ROUND(AVG(csat_score), 2) AS avg_csat,
    CAST(NULL AS DOUBLE) AS sla_breach_pct
FROM marts.fct_case_lifecycle
WHERE resolved_at IS NOT NULL
GROUP BY missing_docs
UNION ALL
SELECT
    'handoffs_vs_csat' AS analysis_type,
    CASE
        WHEN handoff_count <= 1 THEN '0-1_handoffs'
        WHEN handoff_count = 2 THEN '2_handoffs'
        ELSE '3+_handoffs'
    END AS segment,
    COUNT(*) AS case_count,
    ROUND(AVG(days_to_resolution), 2) AS avg_days_to_resolution,
    ROUND(AVG(csat_score), 2) AS avg_csat,
    CAST(NULL AS DOUBLE) AS sla_breach_pct
FROM marts.fct_case_lifecycle
WHERE csat_score IS NOT NULL
GROUP BY 2
UNION ALL
SELECT
    'case_type_vs_sla_breach' AS analysis_type,
    case_type AS segment,
    COUNT(*) AS case_count,
    ROUND(AVG(days_to_resolution), 2) AS avg_days_to_resolution,
    CAST(NULL AS DOUBLE) AS avg_csat,
    ROUND(100.0 * COUNT(*) FILTER (WHERE sla_breach_flag) / NULLIF(COUNT(*), 0), 2) AS sla_breach_pct
FROM marts.fct_case_lifecycle
GROUP BY case_type;
