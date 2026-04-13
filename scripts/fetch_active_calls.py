#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd

from prepare_beats_reference import OUTPUT_CSV as BEATS_REFERENCE_CSV
from prepare_beats_reference import build_beats_reference_frame


ROOT = Path(__file__).resolve().parents[1]
RAW_JSON = ROOT / "data" / "raw" / "Active_Calls.json"
OUTPUT_LATEST = ROOT / "data" / "clean" / "active_calls_latest.csv"
OUTPUT_BY_BEAT = ROOT / "data" / "clean" / "active_calls_by_beat.csv"
OUTPUT_BY_DIVISION = ROOT / "data" / "clean" / "active_calls_by_division.csv"
DB_PATH = ROOT / "dallas_crime.db"
ACTIVE_CALLS_URL = "https://www.dallasopendata.com/resource/9fxf-t2tr.json?$limit=50000"


def build_headers() -> dict[str, str]:
    headers = {"User-Agent": "dallas-crime-intelligence/0.1"}
    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token
    return headers


def fetch_active_calls_json() -> list[dict]:
    request = Request(ACTIVE_CALLS_URL, headers=build_headers())
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def normalize_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )


def build_beats_reference() -> pd.DataFrame:
    if BEATS_REFERENCE_CSV.exists():
        return pd.read_csv(BEATS_REFERENCE_CSV, dtype={"beat": "string", "sector": "string", "division": "string"})
    beats = build_beats_reference_frame()
    BEATS_REFERENCE_CSV.parent.mkdir(parents=True, exist_ok=True)
    beats.to_csv(BEATS_REFERENCE_CSV, index=False)
    return beats


def main() -> int:
    RAW_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_LATEST.parent.mkdir(parents=True, exist_ok=True)

    payload = fetch_active_calls_json()
    with RAW_JSON.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    calls = pd.DataFrame(payload)
    if calls.empty:
        print("Active Calls feed returned no records.")
        return 0

    expected = [
        "incident_number",
        "division",
        "nature_of_call",
        "priority",
        "date",
        "time",
        "unit_number",
        "block",
        "location",
        "beat",
        "reporting_area",
        "status",
    ]
    for column in expected:
        if column not in calls.columns:
            calls[column] = pd.NA

    calls = calls[expected].copy()
    for column in ["incident_number", "division", "nature_of_call", "unit_number", "block", "location", "beat", "reporting_area", "status"]:
        calls[column] = normalize_text(calls[column])

    calls["division"] = calls["division"].str.upper()
    calls["priority"] = pd.to_numeric(calls["priority"], errors="coerce").astype("Int64")
    parsed_dates = pd.to_datetime(calls["date"], errors="coerce")
    calls["call_date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    calls["call_timestamp"] = pd.to_datetime(calls["call_date"].fillna("") + " " + calls["time"].fillna(""), format="%Y-%m-%d %H:%M:%S", errors="coerce")

    beats = build_beats_reference()
    by_beat = (
        calls.assign(priority_1_flag=(calls["priority"] == 1).astype(int))
        .groupby("beat", dropna=False, as_index=False)
        .agg(
            active_calls=("incident_number", "count"),
            priority_1_calls=("priority_1_flag", "sum"),
        )
        .merge(beats, on="beat", how="left")
        .sort_values(["active_calls", "beat"], ascending=[False, True])
    )

    by_division = (
        calls.assign(priority_1_flag=(calls["priority"] == 1).astype(int))
        .groupby("division", dropna=False, as_index=False)
        .agg(
            active_calls=("incident_number", "count"),
            priority_1_calls=("priority_1_flag", "sum"),
        )
        .sort_values(["active_calls", "division"], ascending=[False, True])
    )

    calls.to_csv(OUTPUT_LATEST, index=False)
    by_beat.to_csv(OUTPUT_BY_BEAT, index=False)
    by_division.to_csv(OUTPUT_BY_DIVISION, index=False)

    with sqlite3.connect(DB_PATH) as connection:
        calls.to_sql("active_calls_latest", connection, if_exists="replace", index=False)
        by_beat.to_sql("active_calls_by_beat", connection, if_exists="replace", index=False)
        by_division.to_sql("active_calls_by_division", connection, if_exists="replace", index=False)

    print(f"Saved {OUTPUT_LATEST.relative_to(ROOT)} with {len(calls)} active calls")
    print(f"Saved {OUTPUT_BY_BEAT.relative_to(ROOT)} with {len(by_beat)} beat summaries")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
