#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW_JSON = ROOT / "data" / "raw" / "ZIP_Demographics_ACS_2024.json"
ZIP_CRIME_STATS = ROOT / "data" / "clean" / "zip_crime_stats.csv"
OUTPUT_DEMOGRAPHICS = ROOT / "data" / "clean" / "zip_demographics.csv"
OUTPUT_MERGED = ROOT / "data" / "clean" / "zip_crime_demographics.csv"
OUTPUT_SUMMARY = ROOT / "outputs" / "zip_demographics_summary.txt"
DB_PATH = ROOT / "dallas_crime.db"

ACS_RENAME_MAP = {
    "NAME": "geography_name",
    "zip code tabulation area": "zip_code",
    "B01002_001E": "median_age",
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "below_poverty_count",
    "B23025_003E": "labor_force",
    "B23025_005E": "unemployed_count",
    "B15003_001E": "education_universe_25_plus",
    "B15003_022E": "bachelors_count",
    "B15003_023E": "masters_count",
    "B15003_024E": "professional_school_count",
    "B15003_025E": "doctorate_count",
}

NUMERIC_COLUMNS = [
    "median_age",
    "total_population",
    "median_household_income",
    "poverty_universe",
    "below_poverty_count",
    "labor_force",
    "unemployed_count",
    "education_universe_25_plus",
    "bachelors_count",
    "masters_count",
    "professional_school_count",
    "doctorate_count",
]


def load_raw_acs() -> pd.DataFrame:
    if not RAW_JSON.exists():
        raise FileNotFoundError(f"Missing {RAW_JSON}. Run scripts/download_data.py --dataset zip_demographics_acs first.")

    with RAW_JSON.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not payload:
        raise ValueError("ZIP demographics raw payload is empty.")

    header, *rows = payload
    frame = pd.DataFrame(rows, columns=header).rename(columns=ACS_RENAME_MAP)
    frame["zip_code"] = frame["zip_code"].astype(str).str.extract(r"(\d{5})")[0]
    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["bachelors_plus_count"] = frame[
        ["bachelors_count", "masters_count", "professional_school_count", "doctorate_count"]
    ].sum(axis=1)
    frame["poverty_rate_pct"] = np.where(frame["poverty_universe"] > 0, frame["below_poverty_count"] / frame["poverty_universe"] * 100, np.nan)
    frame["unemployment_rate_pct"] = np.where(frame["labor_force"] > 0, frame["unemployed_count"] / frame["labor_force"] * 100, np.nan)
    frame["bachelors_plus_rate_pct"] = np.where(
        frame["education_universe_25_plus"] > 0,
        frame["bachelors_plus_count"] / frame["education_universe_25_plus"] * 100,
        np.nan,
    )
    return frame


def minmax_score(series: pd.Series, reverse: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    minimum = values.min()
    maximum = values.max()
    if pd.isna(minimum) or pd.isna(maximum) or minimum == maximum:
        scaled = pd.Series(50.0, index=values.index)
    else:
        scaled = ((values - minimum) / (maximum - minimum)) * 100
    return 100 - scaled if reverse else scaled


def build_context_score(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame.copy()
    scored["poverty_score"] = minmax_score(scored["poverty_rate_pct"])
    scored["unemployment_score"] = minmax_score(scored["unemployment_rate_pct"])
    scored["income_score"] = minmax_score(scored["median_household_income"], reverse=True)
    scored["education_score"] = minmax_score(scored["bachelors_plus_rate_pct"], reverse=True)
    scored["context_score"] = scored[["poverty_score", "unemployment_score", "income_score", "education_score"]].mean(axis=1).round(2)
    return scored


def build_regression_summary(frame: pd.DataFrame) -> str:
    analysis = frame.dropna(
        subset=[
            "incidents_per_1000",
            "poverty_rate_pct",
            "unemployment_rate_pct",
            "bachelors_plus_rate_pct",
            "median_household_income",
        ]
    ).copy()

    if analysis.empty:
        return "Not enough joined rows to compute ZIP demographics summary.\n"

    x = analysis[["poverty_rate_pct", "unemployment_rate_pct", "bachelors_plus_rate_pct", "median_household_income"]].to_numpy(dtype=float)
    y = analysis["incidents_per_1000"].to_numpy(dtype=float)
    x = np.column_stack([np.ones(len(x)), x])
    coefficients, *_ = np.linalg.lstsq(x, y, rcond=None)
    y_hat = x @ coefficients
    ss_res = float(((y - y_hat) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r_squared = 1 - ss_res / ss_tot if ss_tot else np.nan

    correlations = analysis[
        ["incidents_per_1000", "poverty_rate_pct", "unemployment_rate_pct", "bachelors_plus_rate_pct", "median_household_income"]
    ].corr(numeric_only=True)["incidents_per_1000"].drop("incidents_per_1000").sort_values(ascending=False)

    lines = [
        "ZIP Demographics Summary",
        f"Rows analyzed: {len(analysis):,}",
        f"Linear regression R^2: {r_squared:.3f}" if not np.isnan(r_squared) else "Linear regression R^2: N/A",
        "",
        "Regression coefficients for incidents_per_1000:",
        f"  Intercept: {coefficients[0]:.4f}",
        f"  Poverty rate pct: {coefficients[1]:.4f}",
        f"  Unemployment rate pct: {coefficients[2]:.4f}",
        f"  Bachelors+ rate pct: {coefficients[3]:.4f}",
        f"  Median household income: {coefficients[4]:.6f}",
        "",
        "Correlations with incidents_per_1000:",
    ]
    lines.extend([f"  {name}: {value:.3f}" for name, value in correlations.items()])
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    if not ZIP_CRIME_STATS.exists():
        raise FileNotFoundError(f"Missing {ZIP_CRIME_STATS}. Run scripts/process_historical.py first.")

    demographics = load_raw_acs()
    zip_crime = pd.read_csv(ZIP_CRIME_STATS, dtype={"zip_code": "string"})

    project_zips = zip_crime["zip_code"].dropna().astype(str).str.extract(r"(\d{5})")[0].dropna().unique().tolist()
    demographics = demographics.loc[demographics["zip_code"].isin(project_zips)].copy()
    demographics = build_context_score(demographics).sort_values("zip_code").reset_index(drop=True)

    zip_crime["zip_code"] = zip_crime["zip_code"].astype(str).str.extract(r"(\d{5})")[0]
    zip_crime["violent_rate"] = zip_crime["violent_crimes"] / zip_crime["total_crimes"]
    zip_crime["nighttime_rate"] = zip_crime["nighttime_crimes"] / zip_crime["total_crimes"]

    merged = zip_crime.merge(demographics, on="zip_code", how="left")
    merged["incidents_per_1000"] = np.where(
        merged["total_population"] > 0,
        merged["total_crimes"] / merged["total_population"] * 1000,
        np.nan,
    )
    merged["violent_incidents_per_1000"] = np.where(
        merged["total_population"] > 0,
        merged["violent_crimes"] / merged["total_population"] * 1000,
        np.nan,
    )
    merged["combined_risk_score"] = merged[["risk_score", "context_score"]].mean(axis=1).round(2)
    merged = merged.sort_values(["year", "combined_risk_score"], ascending=[True, False]).reset_index(drop=True)

    OUTPUT_DEMOGRAPHICS.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    demographics.to_csv(OUTPUT_DEMOGRAPHICS, index=False)
    merged.to_csv(OUTPUT_MERGED, index=False)
    OUTPUT_SUMMARY.write_text(build_regression_summary(merged), encoding="utf-8")

    with sqlite3.connect(DB_PATH) as connection:
        demographics.to_sql("zip_demographics", connection, if_exists="replace", index=False)
        merged.to_sql("zip_crime_demographics", connection, if_exists="replace", index=False)

    print(f"Saved {OUTPUT_DEMOGRAPHICS.relative_to(ROOT)} with {len(demographics)} ZIP rows")
    print(f"Saved {OUTPUT_MERGED.relative_to(ROOT)} with {len(merged)} joined rows")
    print(f"Saved {OUTPUT_SUMMARY.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
