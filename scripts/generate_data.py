"""
Generate synthetic healthcare operations data for Solace Ops Intelligence.
Order: patients -> advocates -> cases -> interactions + case_events -> patient_feedback.
Output: CSV files in data/raw/
"""
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
from faker import Faker

Faker.seed(42)
np.random.seed(42)
random.seed(42)

fake = Faker()
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Config
N_PATIENTS = 1200
N_ADVOCATES = 30
N_CASES = 1500
CASE_TYPES = [
    "prior_authorization",
    "claim_denial",
    "billing_dispute",
    "provider_search",
    "care_coordination",
]
URGENCIES = ["low", "medium", "high"]
STATUSES_OPEN = ["open", "in_progress", "pending_docs", "escalated"]
STATUSES_CLOSED = ["resolved", "reopened"]
SLA_DAYS = 7
STALE_HOURS = 48


def ensure_raw_dir():
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def generate_patients(n: int) -> pd.DataFrame:
    insurance = ["commercial", "medicare", "medicaid", "exchange", "uninsured"]
    channel = ["phone", "email", "chat", "portal"]
    states = [fake.state_abbr() for _ in range(25)]
    rows = []
    for i in range(1, n + 1):
        created = fake.date_time_between(start_date="-2y", end_date="now")
        rows.append({
            "id": i,
            "insurance_type": random.choices(insurance, weights=[35, 25, 20, 15, 5])[0],
            "preferred_channel": random.choice(channel),
            "state": random.choice(states),
            "risk_score": round(np.clip(np.random.normal(0.4, 0.25), 0, 1), 2),
            "created_at": created.isoformat(),
        })
    return pd.DataFrame(rows)


def generate_advocates(n: int) -> pd.DataFrame:
    teams = ["intake", "resolution", "specialty", "escalations"]
    specialties = ["prior_auth", "claims", "billing", "care_coord", "general"]
    rows = []
    for i in range(1, n + 1):
        hire = fake.date_between(start_date="-3y", end_date="-6m")
        capacity = random.choices([15, 20, 25, 30], weights=[20, 40, 30, 10])[0]
        rows.append({
            "id": i,
            "name": fake.name(),
            "specialty": random.choice(specialties),
            "team": random.choice(teams),
            "capacity": capacity,
            "hire_date": hire.isoformat(),
        })
    return pd.DataFrame(rows)


def _case_type_weights():
    return [0.25, 0.25, 0.20, 0.15, 0.15]  # prior_auth, claim_denial, billing, provider, care_coord


def _resolution_days_base(case_type: str, has_missing_docs: bool, escalated: bool, handoffs: int) -> float:
    """Behavioral: prior_auth/claim_denial longer; missing docs and escalations add days."""
    base = {
        "prior_authorization": 8,
        "claim_denial": 9,
        "billing_dispute": 4,
        "provider_search": 5,
        "care_coordination": 6,
    }[case_type]
    if has_missing_docs:
        base += np.random.uniform(2, 5)
    if escalated:
        base += np.random.uniform(1, 3)
    base += handoffs * 0.5
    return max(1, base + np.random.normal(0, 2))


def _first_response_hours(urgency: str) -> float:
    """High urgency -> faster first response."""
    mean = {"high": 4, "medium": 12, "low": 24}[urgency]
    return max(0.5, np.random.exponential(mean / 2) + mean / 2)


def generate_cases(
    n: int,
    patient_ids: list,
    advocate_ids: list,
) -> pd.DataFrame:
    # Bias some advocates to get more cases (overload)
    advocate_weights = np.random.gamma(1.5, 1, size=len(advocate_ids))
    advocate_weights = advocate_weights / advocate_weights.sum()

    rows = []
    for i in range(1, n + 1):
        case_type = random.choices(CASE_TYPES, weights=_case_type_weights())[0]
        urgency = random.choices(URGENCIES, weights=[30, 50, 20])[0]
        is_resolved = random.random() < 0.82
        escalation_flag = random.random() < (0.25 if case_type in ("prior_authorization", "claim_denial") else 0.12)
        reopen_flag = is_resolved and random.random() < 0.08

        handoff_count = int(np.random.poisson(1.2))
        if escalation_flag:
            handoff_count = max(handoff_count, 1)
        complexity = random.choices(["low", "medium", "high"], weights=[40, 45, 15])[0]
        missing_docs = random.random() < 0.35
        submitted_docs = [] if missing_docs else [fake.file_name(extension="pdf") for _ in range(random.randint(1, 4))]
        intake_source = random.choice(["phone", "portal", "referral", "email"])
        barriers = random.sample(
            ["language", "transportation", "health_literacy", "insurance_confusion", "none"],
            k=random.randint(1, 3),
        )
        if "none" in barriers and len(barriers) > 1:
            barriers = [b for b in barriers if b != "none"]
        support_flags = [b for b in barriers if b in ("language", "transportation")]

        metadata = {
            "intake_source": intake_source,
            "barriers": barriers,
            "handoff_count": handoff_count,
            "complexity": complexity,
            "submitted_documents": submitted_docs,
            "support_flags": support_flags,
            "missing_docs": missing_docs,
        }

        created = fake.date_time_between(start_date="-1y", end_date="now")
        resolution_days = _resolution_days_base(case_type, missing_docs, escalation_flag, handoff_count)
        assigned_delta = timedelta(hours=random.uniform(0.5, 4))
        assigned_at = created + assigned_delta
        first_response_h = _first_response_hours(urgency)
        first_response_at = assigned_at + timedelta(hours=first_response_h)

        if is_resolved:
            resolved_at = created + timedelta(days=resolution_days)
            if resolved_at > datetime.now():
                resolved_at = datetime.now() - timedelta(days=random.randint(0, 3))
            status = "reopened" if reopen_flag else "resolved"
        else:
            resolved_at = None
            status = random.choice(STATUSES_OPEN)

        advocate_id = int(np.random.choice(advocate_ids, p=advocate_weights))
        patient_id = random.choice(patient_ids)

        rows.append({
            "id": i,
            "patient_id": patient_id,
            "advocate_id": advocate_id,
            "case_type": case_type,
            "urgency": urgency,
            "status": status,
            "created_at": created.isoformat(),
            "assigned_at": assigned_at.isoformat(),
            "first_response_at": first_response_at.isoformat() if is_resolved else "",
            "resolved_at": resolved_at.isoformat() if resolved_at else "",
            "escalation_flag": 1 if escalation_flag else 0,
            "reopen_flag": 1 if reopen_flag else 0,
            "metadata_json": json.dumps(metadata),
        })

    return pd.DataFrame(rows)


def generate_interactions_and_events(cases_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    interaction_types = ["call", "chat", "email", "doc_request", "insurer_followup"]
    event_types = ["opened", "assigned", "first_response", "docs_requested", "docs_received", "escalated", "resolved", "reopened"]

    interactions_rows = []
    events_rows = []
    interaction_id = 1
    event_id = 1

    for _, c in cases_df.iterrows():
        case_id = c["id"]
        created = datetime.fromisoformat(c["created_at"])
        assigned = datetime.fromisoformat(c["assigned_at"])
        advocate_id = c["advocate_id"]
        case_type = c["case_type"]
        escalation_flag = c["escalation_flag"]
        metadata = json.loads(c["metadata_json"])
        handoff_count = metadata.get("handoff_count", 0)
        missing_docs = metadata.get("missing_docs", False)
        resolved_at = c["resolved_at"]
        first_response_at = c["first_response_at"]
        is_resolved = bool(resolved_at)
        reopen_flag = c["reopen_flag"]

        # Events: opened, assigned
        events_rows.append({"id": event_id, "case_id": case_id, "event_type": "opened", "occurred_at": created.isoformat()})
        event_id += 1
        events_rows.append({"id": event_id, "case_id": case_id, "event_type": "assigned", "occurred_at": assigned.isoformat()})
        event_id += 1

        # First response interaction + event
        fr_at = datetime.fromisoformat(first_response_at) if first_response_at else assigned + timedelta(hours=12)
        duration = random.uniform(5, 25)
        interactions_rows.append({
            "id": interaction_id, "case_id": case_id, "type": "call",
            "created_at": fr_at.isoformat(), "duration_minutes": round(duration, 1), "advocate_id": advocate_id,
        })
        interaction_id += 1
        events_rows.append({"id": event_id, "case_id": case_id, "event_type": "first_response", "occurred_at": fr_at.isoformat()})
        event_id += 1

        n_extra = int(np.random.poisson(3))
        if case_type in ("prior_authorization", "claim_denial"):
            n_extra += random.randint(1, 3)
        if missing_docs:
            events_rows.append({
                "id": event_id, "case_id": case_id, "event_type": "docs_requested",
                "occurred_at": (fr_at + timedelta(hours=2)).isoformat(),
            })
            event_id += 1
            interactions_rows.append({
                "id": interaction_id, "case_id": case_id, "type": "doc_request",
                "created_at": (fr_at + timedelta(hours=2)).isoformat(), "duration_minutes": 0, "advocate_id": advocate_id,
            })
            interaction_id += 1
            if is_resolved:
                doc_received = datetime.fromisoformat(resolved_at) - timedelta(days=1)
                events_rows.append({"id": event_id, "case_id": case_id, "event_type": "docs_received", "occurred_at": doc_received.isoformat()})
                event_id += 1

        if escalation_flag:
            esc_at = fr_at + timedelta(days=random.uniform(1, 3))
            events_rows.append({"id": event_id, "case_id": case_id, "event_type": "escalated", "occurred_at": esc_at.isoformat()})
            event_id += 1
            interactions_rows.append({
                "id": interaction_id, "case_id": case_id, "type": "insurer_followup",
                "created_at": esc_at.isoformat(), "duration_minutes": round(random.uniform(10, 40), 1), "advocate_id": advocate_id,
            })
            interaction_id += 1

        # Additional interactions
        for _ in range(n_extra):
            delta = timedelta(hours=random.uniform(2, 72))
            t = fr_at + delta
            if is_resolved and resolved_at:
                res_dt = datetime.fromisoformat(resolved_at)
                if t >= res_dt:
                    break
            itype = random.choices(interaction_types, weights=[30, 20, 25, 10, 15])[0]
            interactions_rows.append({
                "id": interaction_id, "case_id": case_id, "type": itype,
                "created_at": t.isoformat(), "duration_minutes": round(random.uniform(3, 30), 1) if itype != "doc_request" else 0,
                "advocate_id": advocate_id,
            })
            interaction_id += 1

        if is_resolved:
            res_dt = datetime.fromisoformat(resolved_at)
            events_rows.append({"id": event_id, "case_id": case_id, "event_type": "resolved", "occurred_at": resolved_at})
            event_id += 1
            if reopen_flag:
                reopen_at = res_dt + timedelta(days=random.uniform(2, 14))
                events_rows.append({"id": event_id, "case_id": case_id, "event_type": "reopened", "occurred_at": reopen_at.isoformat()})
                event_id += 1

    return pd.DataFrame(interactions_rows), pd.DataFrame(events_rows)


def generate_feedback(cases_df: pd.DataFrame) -> pd.DataFrame:
    resolved = cases_df[cases_df["status"].isin(["resolved", "reopened"])].copy()
    resolved = resolved[resolved["resolved_at"].notna() & (resolved["resolved_at"] != "")]
    rows = []
    for i, (_, c) in enumerate(resolved.iterrows(), start=1):
        case_id = c["id"]
        escalation_flag = c["escalation_flag"]
        reopen_flag = c["reopen_flag"]
        metadata = json.loads(c["metadata_json"])
        handoffs = metadata.get("handoff_count", 0)

        # Escalated / reopened / more handoffs -> lower CSAT
        base_csat = 4.0
        if escalation_flag:
            base_csat -= 0.6
        if reopen_flag:
            base_csat -= 0.8
        base_csat -= handoffs * 0.15
        csat = max(1, min(5, round(base_csat + np.random.normal(0, 0.4), 1)))
        nps_bucket = "promoter" if csat >= 4.5 else ("passive" if csat >= 3.5 else "detractor")
        resolved_at = datetime.fromisoformat(c["resolved_at"])
        submitted_at = resolved_at + timedelta(hours=random.uniform(2, 72))
        rows.append({
            "id": i,
            "case_id": case_id,
            "csat_score": csat,
            "nps_bucket": nps_bucket,
            "feedback_text": fake.sentence(nb_words=8) if random.random() < 0.6 else "",
            "submitted_at": submitted_at.isoformat(),
        })
    return pd.DataFrame(rows)


def main():
    ensure_raw_dir()
    print("Generating patients...")
    patients = generate_patients(N_PATIENTS)
    patients.to_csv(RAW_DIR / "patients.csv", index=False)
    patient_ids = patients["id"].tolist()

    print("Generating advocates...")
    advocates = generate_advocates(N_ADVOCATES)
    advocates.to_csv(RAW_DIR / "advocates.csv", index=False)
    advocate_ids = advocates["id"].tolist()

    print("Generating cases...")
    cases = generate_cases(N_CASES, patient_ids, advocate_ids)
    cases.to_csv(RAW_DIR / "cases.csv", index=False)

    print("Generating interactions and case_events...")
    interactions, case_events = generate_interactions_and_events(cases)
    interactions.to_csv(RAW_DIR / "interactions.csv", index=False)
    case_events.to_csv(RAW_DIR / "case_events.csv", index=False)

    print("Generating patient_feedback...")
    feedback = generate_feedback(cases)
    feedback.to_csv(RAW_DIR / "patient_feedback.csv", index=False)

    print(f"Done. Wrote 6 CSVs to {RAW_DIR}")
    print(f"  patients: {len(patients)}, advocates: {len(advocates)}, cases: {len(cases)}")
    print(f"  interactions: {len(interactions)}, case_events: {len(case_events)}, feedback: {len(feedback)}")


if __name__ == "__main__":
    main()
