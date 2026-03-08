"""
Solace Ops Intelligence: Patient Advocacy Funnel + Bottleneck Detection.
Dashboard with Executive, Operations Manager, and Root Cause views.
"""
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# App's project root = folder that contains app/ and scripts/ (single source of truth for DB path)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_NAME = "solace_ops.duckdb"


def _db_path() -> Path:
    """Always use DB next to the app so pipeline and app use the same file."""
    return PROJECT_ROOT / DB_NAME


def get_connection():
    """Open a fresh connection each time so we see the latest DB state after pipeline runs."""
    db_path = _db_path()
    if not db_path.exists():
        return None
    return duckdb.connect(str(db_path), read_only=True)


def marts_ready(conn) -> bool:
    """Return True if pipeline has been run and marts exist."""
    if conn is None:
        return False
    try:
        conn.execute("SELECT 1 FROM marts.mart_ops_kpis LIMIT 1")
        return True
    except duckdb.CatalogException:
        return False


def load_df(conn, query: str) -> pd.DataFrame:
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(query).fetchdf()
    except duckdb.CatalogException:
        return pd.DataFrame()


def main():
    st.set_page_config(page_title="Solace Ops Intelligence", layout="wide")
    st.title("Solace Ops Intelligence")
    st.caption("Patient Advocacy Funnel & Bottleneck Detection")

    conn = get_connection()
    if conn is None:
        st.error("Database not found. Run: `python scripts/generate_data.py`, then `python scripts/load_duckdb.py`, then `python scripts/run_pipeline.py`.")
        return
    if not marts_ready(conn):
        db_path = _db_path()
        st.error("Marts not found — pipeline has not been run (or database was replaced).")
        st.info(f"Database (app folder): `{db_path}`. Pipeline must run in same folder: `{PROJECT_ROOT}`.")
        run_clicked = st.button("**Run pipeline now** (load raw data + build marts)", type="primary", use_container_width=True)
        if run_clicked:
            conn.close()
            with st.spinner("Running load_duckdb.py…"):
                r1 = subprocess.run(
                    [sys.executable, "scripts/load_duckdb.py"],
                    cwd=str(PROJECT_ROOT),
                    capture_output=True,
                    text=True,
                )
            if r1.returncode != 0:
                st.error(f"load_duckdb failed: {r1.stderr or r1.stdout}")
                return
            with st.spinner("Running run_pipeline.py…"):
                r2 = subprocess.run(
                    [sys.executable, "scripts/run_pipeline.py"],
                    cwd=str(PROJECT_ROOT),
                    capture_output=True,
                    text=True,
                )
            if r2.returncode != 0:
                st.error(f"run_pipeline failed: {r2.stderr or r2.stdout}")
                return
            st.success("Pipeline complete. Refreshing…")
            st.rerun()
        # Manual refresh in case the DB was updated from terminal
        if st.button("Refresh page (recheck database)"):
            st.rerun()
        st.caption("Or in a terminal: cd to the folder above, stop Streamlit, then run `python scripts/load_duckdb.py` and `python scripts/run_pipeline.py`. Then click **Refresh page** or reload the browser (F5).")
        return

    tab1, tab2, tab3 = st.tabs(["Executive View", "Operations Manager View", "Root Cause View"])

    # --- Executive View ---
    with tab1:
        st.subheader("Executive View")
        kpis = load_df(conn, "SELECT * FROM marts.mart_ops_kpis")
        fct = load_df(conn, "SELECT * FROM marts.fct_case_lifecycle")
        bottlenecks = load_df(conn, "SELECT * FROM marts.mart_bottlenecks")

        if not fct.empty:
            active = fct[fct["status"].isin(["open", "in_progress", "pending_docs", "escalated"])]
            total_active = len(active)
            avg_resolution = fct["days_to_resolution"].dropna().mean()
            sla_rate = 100 * fct["sla_breach_flag"].sum() / len(fct)
            avg_csat = fct["csat_score"].dropna().mean()
            stale = fct["stale_case_flag"].sum()

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total active cases", total_active)
            c2.metric("Avg resolution (days)", f"{avg_resolution:.1f}" if not pd.isna(avg_resolution) else "—")
            c3.metric("SLA breach rate %", f"{sla_rate:.1f}")
            c4.metric("Avg CSAT", f"{avg_csat:.2f}" if not pd.isna(avg_csat) else "—")
            c5.metric("Stale cases (48h+)", int(stale))

        st.subheader("Backlog by case type")
        if not kpis.empty:
            st.bar_chart(kpis.set_index("case_type")[["open_backlog", "active_cases"]])
        st.subheader("KPIs by case type")
        if not kpis.empty:
            st.dataframe(kpis, use_container_width=True, hide_index=True)

        st.subheader("Top bottlenecks")
        if not bottlenecks.empty:
            st.dataframe(bottlenecks, use_container_width=True, hide_index=True)

    # --- Operations Manager View ---
    with tab2:
        st.subheader("Operations Manager View")
        fct = load_df(conn, "SELECT * FROM marts.fct_case_lifecycle")
        caseload = load_df(conn, "SELECT * FROM marts.mart_advocate_caseload")

        open_cases = fct[fct["status"].isin(["open", "in_progress", "pending_docs", "escalated"])] if not fct.empty else pd.DataFrame()
        if not open_cases.empty:
            st.subheader("Open cases by urgency")
            by_urgency = open_cases.groupby("urgency").size().reindex(["high", "medium", "low"], fill_value=0)
            st.bar_chart(by_urgency)
            st.subheader("Stale cases (no touch 48h+)")
            stale_df = fct[fct["stale_case_flag"]][["case_id", "case_type", "urgency", "status", "created_at", "last_interaction_at"]]
            st.dataframe(stale_df.head(50), use_container_width=True, hide_index=True)
        st.subheader("Advocate caseload")
        if not caseload.empty:
            st.dataframe(caseload, use_container_width=True, hide_index=True)
            over = caseload[caseload["utilization_ratio"] > 1.0]
            if not over.empty:
                st.warning(f"Advocates over capacity: {', '.join(over['name'].astype(str))}")
        st.subheader("Escalation distribution")
        if not fct.empty:
            esc = fct.groupby("case_type")["escalation_flag"].agg(["sum", "count"]).assign(rate=lambda x: 100 * x["sum"] / x["count"])
            st.dataframe(esc, use_container_width=True, hide_index=True)
        st.subheader("Interaction load by case type")
        if not fct.empty:
            interaction_load = fct.groupby("case_type").agg(avg_interactions=("interaction_count", "mean"), cases=("case_id", "count")).reset_index()
            st.bar_chart(interaction_load.set_index("case_type")["avg_interactions"])

    # --- Root Cause View ---
    with tab3:
        st.subheader("Root Cause View")
        fct = load_df(conn, "SELECT * FROM marts.fct_case_lifecycle")
        bottlenecks = load_df(conn, "SELECT * FROM marts.mart_bottlenecks")

        if not fct.empty:
            st.subheader("Missing docs vs resolution time")
            md = fct[fct["resolved_at"].notna()].groupby("missing_docs")["days_to_resolution"].agg(["mean", "count"])
            md.columns = ["avg_days_to_resolution", "case_count"]
            st.dataframe(md, use_container_width=True, hide_index=True)
            st.bar_chart(md["avg_days_to_resolution"])

            st.subheader("Handoff count vs CSAT")
            hc = fct[fct["csat_score"].notna()].groupby("handoff_count").agg(avg_csat=("csat_score", "mean"), case_count=("case_id", "count")).reset_index()
            st.dataframe(hc, use_container_width=True, hide_index=True)
            st.bar_chart(hc.set_index("handoff_count")["avg_csat"])

            st.subheader("Escalations vs SLA breach")
            esc_sla = fct.groupby("escalation_flag").agg(sla_breach_rate=("sla_breach_flag", "mean"), case_count=("case_id", "count"))
            st.dataframe(esc_sla, use_container_width=True, hide_index=True)

            st.subheader("Case type vs delay (avg days to resolution)")
            ct_delay = fct[fct["days_to_resolution"].notna()].groupby("case_type")["days_to_resolution"].mean()
            st.bar_chart(ct_delay)

            st.subheader("Advocate / team comparison (avg resolution days)")
            adv = load_df(conn, """
                SELECT a.id AS advocate_id, a.name, a.team, f.avg_days
                FROM (SELECT advocate_id, AVG(days_to_resolution) AS avg_days FROM marts.fct_case_lifecycle WHERE days_to_resolution IS NOT NULL GROUP BY advocate_id) f
                JOIN staging.stg_advocates a ON a.id = f.advocate_id
            """)
            if not adv.empty:
                st.dataframe(adv, use_container_width=True, hide_index=True)
        if not bottlenecks.empty:
            st.subheader("Bottleneck summary")
            st.dataframe(bottlenecks, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
