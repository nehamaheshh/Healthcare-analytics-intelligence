# Solace Ops Intelligence

**Patient Advocacy Funnel & Bottleneck Detection System**

An internal analytics product for a healthcare advocacy startup that helps patients navigate billing disputes, prior authorizations, provider search, care coordination, and claim denials. This project helps operations leaders answer:

- Which case types are slowing the team down
- Where patients are getting stuck
- Which advocates are overloaded
- What drives SLA breaches
- What workflow changes would improve resolution time and satisfaction

---

## Business context

The system simulates a **patient advocacy case lifecycle**: a patient opens a case → case is assigned to an advocate → advocate responds → documents may be requested → advocate contacts provider or insurer → case may be escalated → case is resolved or reopened → patient submits feedback.

Analytics are built around this funnel so that KPIs (response time, resolution time, escalation rate, CSAT, caseload) map directly to operational decisions.

---

## Architecture

```
Business problem → synthetic raw data → cleaned/staged data → analytics tables → dashboard → insights → recommendations
```

1. **Synthetic data** (Python): patients, advocates, cases, interactions, case_events, patient_feedback — with behaviorally realistic patterns (e.g. prior auth/claim denial take longer; missing docs increase resolution time; escalated cases have lower CSAT).
2. **Raw layer**: CSVs in `data/raw/` loaded into DuckDB as `raw.*` tables.
3. **Staging**: Cleaned and typed; **JSON in cases** (intake_source, barriers, handoff_count, complexity, submitted_documents, support_flags) is parsed into structured columns.
4. **Intermediate**: Case-level timings (hours to first response, days to resolution, interaction counts).
5. **Marts**: `fct_case_lifecycle` (one row per case with SLA/stale flags, CSAT), `mart_ops_kpis` (by case type), `mart_advocate_caseload`, `mart_bottlenecks`.
6. **Dashboard**: Streamlit with Executive, Operations Manager, and Root Cause views.
7. **Bottleneck logic**: Missing docs vs resolution, handoffs vs CSAT, case type vs SLA breach, advocate overload — surfaced in SQL and dashboard.

---

## Data model

| Layer   | Tables |
|---------|--------|
| **Raw** | patients, advocates, cases, interactions, case_events, patient_feedback |
| **Staging** | stg_patients, stg_advocates, stg_cases (with parsed JSON), stg_interactions, stg_case_events, stg_feedback |
| **Intermediate** | int_case_timings |
| **Marts** | fct_case_lifecycle, mart_ops_kpis, mart_advocate_caseload, mart_bottlenecks |

**Cases** include a `metadata_json` column with nested fields (intake_source, barriers, handoff_count, complexity, submitted_documents, support_flags, missing_docs). Staging parses these into columns for analysis.

---

## KPI definitions

| KPI | Definition | Business interpretation |
|-----|-------------|-------------------------|
| Total / active cases | All cases; open + in progress + pending_docs + escalated | Volume and backlog |
| Avg hours to first response | Time from case creation to first_response event | Intake and assignment efficiency |
| Avg days to resolution | Created to resolved (resolved cases only) | Ops throughput |
| Escalation rate | % of cases with escalation_flag | Complexity and process friction |
| Reopen rate | % of resolved cases that were reopened | Resolution quality |
| SLA breach rate | % of cases that exceeded 7-day SLA (resolved or open) | Timeliness risk |
| Avg interactions per case | Mean interaction count per case | Touchpoint intensity |
| Avg CSAT | Mean post-resolution CSAT | Patient experience |
| Stale cases | Open cases with no touch in 48+ hours | Follow-up breakdown |
| Caseload per advocate | Active cases per advocate vs capacity | Staffing balance |

---

## Transformation layers

- **Raw**: Untouched after ingestion; CSVs loaded into DuckDB.
- **Staging**: Type casting, category normalization, **JSON parsing** for cases (handoff_count, complexity, missing_docs, intake_source, barriers, support_flags).
- **Intermediate**: Joins of cases, events, interactions to compute hours_to_first_response, days_to_resolution, interaction_count, last_interaction_at, avg duration.
- **Marts**: Fact table with SLA and stale flags; aggregate KPIs by case type; advocate caseload; bottleneck segments (missing_docs vs resolution, handoffs vs CSAT, case_type vs SLA breach).

---

## Dashboard views

- **Executive View**: Total active cases, avg resolution days, SLA breach rate, CSAT, stale count; backlog by case type; KPIs by case type; top bottlenecks table.
- **Operations Manager View**: Open cases by urgency; stale cases list; advocate caseload and overload warning; escalation distribution; interaction load by case type.
- **Root Cause View**: Missing docs vs resolution time; handoff count vs CSAT; escalations vs SLA breach; case type vs delay; advocate/team comparison; bottleneck summary.

---

## Sample insights (from synthetic data)

- **Prior authorization and claim denial** cases have longer resolution and higher escalation rates.
- **Missing documents** are associated with longer resolution times.
- **More handoffs** correlate with lower CSAT.
- **Escalated cases** have higher SLA breach rates and lower satisfaction.
- **Caseload imbalance**: some advocates have active cases above capacity (utilization_ratio > 1).

---

## Recommendations

- Reduce handoffs (e.g. single-advocate ownership where possible) to improve CSAT.
- Address missing documents early (checklists, reminders) to shorten resolution time.
- Balance caseload (reassign or hire) where utilization exceeds capacity.
- Prioritize prior auth and claim denial workflows for process improvement and SLA focus.
- Use stale-case list for daily follow-up to avoid 48h+ gaps.

---

## How to run

**Prerequisites:** Python 3.9+, `pip install -r requirements.txt`

```bash
# 1. Generate synthetic data (CSVs in data/raw/)
python scripts/generate_data.py

# 2. Load raw data into DuckDB
python scripts/load_duckdb.py

# 3. Run SQL pipeline (staging → intermediate → marts)
python scripts/run_pipeline.py

# 4. Launch dashboard
streamlit run app/streamlit_app.py
```

Single sequence: run 1–3 once, then 4 to view the dashboard. Regenerate data and re-run 2–3 to refresh.

---

## Repo structure

```
Solace_project/
├── data/
│   ├── raw/          # patients.csv, advocates.csv, cases.csv, interactions.csv, case_events.csv, patient_feedback.csv
│   └── processed/
├── sql/
│   ├── staging/      # stg_*.sql
│   ├── intermediate/ # int_case_timings.sql
│   └── marts/        # fct_case_lifecycle.sql, mart_ops_kpis.sql, mart_advocate_caseload.sql, mart_bottlenecks.sql
├── scripts/
│   ├── generate_data.py
│   ├── load_duckdb.py
│   └── run_pipeline.py
├── app/
│   └── streamlit_app.py
├── notebooks/
├── README.md
├── requirements.txt
└── solace_ops.duckdb   # created by load_duckdb.py
```

---

## Future enhancements

- **SLA risk model**: Logistic regression or gradient boosting to predict open-case SLA breach probability; queue of cases needing intervention.
- **Advocate utilization**: Weighted caseload (e.g. by complexity) vs capacity; overload flags and staffing recommendations.
- **Real-time ingestion**: Replace synthetic batch with live event stream and incremental models.
- **Alerts**: Notifications when stale cases or advocate overload exceed thresholds.
