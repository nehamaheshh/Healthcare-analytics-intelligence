-- Mart: advocate caseload (active cases per advocate) for operations view.
CREATE OR REPLACE TABLE marts.mart_advocate_caseload AS
SELECT
    a.id AS advocate_id,
    a.name,
    a.team,
    a.specialty,
    a.capacity,
    COUNT(f.case_id) FILTER (WHERE f.status NOT IN ('resolved', 'reopened')) AS active_cases,
    ROUND(
        COUNT(f.case_id) FILTER (WHERE f.status NOT IN ('resolved', 'reopened'))
        * 1.0 / NULLIF(a.capacity, 0),
        2
    ) AS utilization_ratio
FROM staging.stg_advocates a
LEFT JOIN marts.fct_case_lifecycle f ON f.advocate_id = a.id
GROUP BY a.id, a.name, a.team, a.specialty, a.capacity;
