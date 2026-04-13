#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW_INCIDENTS = ROOT / "data" / "raw" / "Police_Incidents.csv"
OUTPUT_DIR = ROOT / "data" / "clean"
DB_PATH = ROOT / "dallas_crime.db"

USECOLS = [
    "Incident Number w/year",
    "Incident Address",
    "Date1 of Occurrence",
    "Time1 of Occurrence",
    "Division",
    "Beat",
    "Sector",
    "Offense Status",
    "Offense Type",
    "Type of Incident",
    "UCR Offense Name",
    "NIBRS Crime Category",
    "Zip Code",
    "Location1",
]

RENAME_MAP = {
    "Incident Number w/year": "incident_number",
    "Incident Address": "incident_address",
    "Date1 of Occurrence": "date1_of_occurrence",
    "Time1 of Occurrence": "time1_of_occurrence",
    "Division": "division",
    "Beat": "beat",
    "Sector": "sector",
    "Offense Status": "offense_status",
    "Offense Type": "offense_type_raw",
    "Type of Incident": "type_of_incident",
    "UCR Offense Name": "ucr_offense_name",
    "NIBRS Crime Category": "nibrs_crime_category",
    "Zip Code": "zip_code",
    "Location1": "location1",
}

CLEAN_COLUMNS = [
    "incident_number",
    "date",
    "year",
    "month",
    "month_year",
    "day_of_week",
    "hour",
    "offense_type",
    "division",
    "beat",
    "sector",
    "latitude",
    "longitude",
    "address",
    "zip_code",
    "ucr_offense",
    "crime_category",
    "time_of_day",
    "offense_status",
]

VIOLENT_PATTERN = r"ASSAULT|ROBBERY|HOMICIDE|MURDER|RAPE|KIDNAPP|SEXUAL ASSAULT|AGGRAVATED ASSAULT"
PROPERTY_PATTERN = r"BURGLARY|THEFT|LARCENY|SHOPLIFT|AUTO THEFT|MOTOR VEHICLE THEFT|ARSON|BURGLARY OF VEHICLE|STOLEN VEHICLE"


def normalize_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )


def combine_grouped(frames: list[pd.DataFrame], keys: list[str], sum_cols: list[str]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=keys + sum_cols)

    combined = pd.concat(frames, ignore_index=True)
    agg_map = {column: "sum" for column in sum_cols}
    return combined.groupby(keys, as_index=False).agg(agg_map)


def build_clean_chunk(raw_chunk: pd.DataFrame, start_year: int, end_year: int) -> pd.DataFrame:
    chunk = raw_chunk.rename(columns=RENAME_MAP).copy()

    for column in chunk.columns:
        chunk[column] = normalize_text(chunk[column])

    occurrence_date = pd.to_datetime(chunk["date1_of_occurrence"], errors="coerce")
    year = occurrence_date.dt.year.astype("Int64")
    month = occurrence_date.dt.month.astype("Int64")
    hour = pd.to_numeric(chunk["time1_of_occurrence"].str.extract(r"^(\d{1,2})")[0], errors="coerce").astype("Int64")

    division = normalize_text(chunk["division"]).str.upper()
    beat = normalize_text(chunk["beat"])
    sector = normalize_text(chunk["sector"])
    offense_status = normalize_text(chunk["offense_status"])

    offense_type = normalize_text(chunk["offense_type_raw"])
    offense_type = offense_type.combine_first(normalize_text(chunk["type_of_incident"]))
    offense_type = offense_type.combine_first(normalize_text(chunk["ucr_offense_name"]))
    offense_type = offense_type.fillna("UNKNOWN")

    ucr_offense = normalize_text(chunk["ucr_offense_name"]).combine_first(offense_type)
    location1 = normalize_text(chunk["location1"])
    address = normalize_text(chunk["incident_address"]).combine_first(location1.str.split("\n").str[0])
    zip_code = normalize_text(chunk["zip_code"]).str.extract(r"(\d{5})")[0]

    coords = location1.fillna("").str.extract(r"\(([-\d.]+)\s*,\s*([-\d.]+)\)")
    latitude = pd.to_numeric(coords[0], errors="coerce")
    longitude = pd.to_numeric(coords[1], errors="coerce")

    crime_text = (
        offense_type.fillna("")
        + " "
        + ucr_offense.fillna("")
        + " "
        + normalize_text(chunk["nibrs_crime_category"]).fillna("")
    ).str.upper()
    violent_mask = crime_text.str.contains(VIOLENT_PATTERN, case=False, na=False)
    property_mask = crime_text.str.contains(PROPERTY_PATTERN, case=False, na=False)

    cleaned = pd.DataFrame(
        {
            "incident_number": normalize_text(chunk["incident_number"]),
            "date": occurrence_date.dt.strftime("%Y-%m-%d"),
            "year": year,
            "month": month,
            "month_year": occurrence_date.dt.to_period("M").astype(str),
            "day_of_week": occurrence_date.dt.day_name(),
            "hour": hour,
            "offense_type": offense_type,
            "division": division,
            "beat": beat,
            "sector": sector,
            "latitude": latitude,
            "longitude": longitude,
            "address": address,
            "zip_code": zip_code,
            "ucr_offense": ucr_offense,
            "crime_category": np.select([violent_mask, property_mask], ["Violent", "Property"], default="Other"),
            "time_of_day": np.select(
                [hour.isna(), hour.between(6, 18, inclusive="both")],
                ["Unknown", "Daytime"],
                default="Nighttime",
            ),
            "offense_status": offense_status,
        }
    )

    valid_mask = cleaned["date"].notna() & cleaned["division"].notna() & cleaned["year"].between(start_year, end_year)
    cleaned = cleaned.loc[valid_mask, CLEAN_COLUMNS].copy()
    return cleaned


def aggregate_clean_chunk(cleaned: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    working = cleaned.copy()
    working["violent_flag"] = (working["crime_category"] == "Violent").astype(int)
    working["property_flag"] = (working["crime_category"] == "Property").astype(int)
    working["nighttime_flag"] = (working["time_of_day"] == "Nighttime").astype(int)

    division_stats = (
        working.groupby(["division", "year"], as_index=False)
        .agg(
            total_crimes=("incident_number", "count"),
            violent_crimes=("violent_flag", "sum"),
            property_crimes=("property_flag", "sum"),
            nighttime_crimes=("nighttime_flag", "sum"),
        )
    )

    monthly = (
        working.groupby(["division", "year", "month", "month_year"], as_index=False)
        .agg(incident_count=("incident_number", "count"))
    )

    crime_category_year = (
        working.groupby(["year", "crime_category"], as_index=False)
        .agg(incident_count=("incident_number", "count"))
    )

    offense_year = (
        working.groupby(["crime_category", "offense_type", "year"], as_index=False)
        .agg(incident_count=("incident_number", "count"))
    )

    zip_stats = (
        working.dropna(subset=["zip_code"])
        .groupby(["zip_code", "year"], as_index=False)
        .agg(
            total_crimes=("incident_number", "count"),
            violent_crimes=("violent_flag", "sum"),
            property_crimes=("property_flag", "sum"),
            nighttime_crimes=("nighttime_flag", "sum"),
        )
    )

    return division_stats, monthly, crime_category_year, offense_year, zip_stats


def add_risk_scores(division_stats: pd.DataFrame) -> pd.DataFrame:
    division_stats = division_stats.copy()
    division_stats["violent_rate"] = division_stats["violent_crimes"] / division_stats["total_crimes"]
    division_stats["nighttime_rate"] = division_stats["nighttime_crimes"] / division_stats["total_crimes"]

    scaled_columns = []
    for column in ["total_crimes", "violent_rate", "nighttime_rate"]:
        minimum = division_stats[column].min()
        maximum = division_stats[column].max()
        if pd.isna(minimum) or pd.isna(maximum) or minimum == maximum:
            scaled = pd.Series(50.0, index=division_stats.index)
        else:
            scaled = ((division_stats[column] - minimum) / (maximum - minimum)) * 100
        scaled_columns.append(scaled)

    division_stats["risk_score"] = pd.concat(scaled_columns, axis=1).mean(axis=1).round(2)
    return division_stats


def build_division_trends(division_stats: pd.DataFrame) -> pd.DataFrame:
    latest_year = int(division_stats["year"].max())
    baseline_year = int(division_stats["year"].min())
    comparison_year = max(baseline_year, latest_year - 2)

    latest = division_stats.loc[division_stats["year"] == latest_year, ["division", "total_crimes", "risk_score"]]
    latest = latest.rename(columns={"total_crimes": f"total_crimes_{latest_year}", "risk_score": "risk_score"})

    comparison = division_stats.loc[division_stats["year"] == comparison_year, ["division", "total_crimes"]]
    comparison = comparison.rename(columns={"total_crimes": f"total_crimes_{comparison_year}"})

    baseline = division_stats.loc[division_stats["year"] == baseline_year, ["division", "total_crimes"]]
    baseline = baseline.rename(columns={"total_crimes": f"total_crimes_{baseline_year}"})

    trend = latest.merge(comparison, on="division", how="left").merge(baseline, on="division", how="left")
    prior_col = f"total_crimes_{comparison_year}"
    baseline_col = f"total_crimes_{baseline_year}"
    latest_col = f"total_crimes_{latest_year}"

    trend["crime_change_pct"] = np.where(
        trend[prior_col].fillna(0) > 0,
        ((trend[latest_col] - trend[prior_col]) / trend[prior_col]) * 100,
        np.nan,
    )
    trend["pct_change_since_baseline"] = np.where(
        trend[baseline_col].fillna(0) > 0,
        ((trend[latest_col] - trend[baseline_col]) / trend[baseline_col]) * 100,
        np.nan,
    )
    trend["trend_label"] = np.select(
        [trend["crime_change_pct"] > 5, trend["crime_change_pct"] < -5],
        ["Worsening", "Improving"],
        default="Stable",
    )
    trend["trend_arrow"] = trend["trend_label"].map({"Worsening": "↑", "Improving": "↓", "Stable": "→"}).fillna("→")
    trend["latest_year"] = latest_year
    trend["comparison_year"] = comparison_year
    trend["baseline_year"] = baseline_year
    return trend.sort_values("crime_change_pct", ascending=False, na_position="last").reset_index(drop=True)


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def create_indexes(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_crimes_year ON crimes(year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_crimes_division ON crimes(division)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_crimes_zip_code ON crimes(zip_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_crimes_month_year ON crimes(month_year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_division_stats_year ON division_stats(year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zip_crime_stats_year ON zip_crime_stats(year)")
    connection.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Dallas Police Incidents into clean analytics outputs.")
    parser.add_argument("--chunk-size", type=int, default=100000, help="Rows per pandas chunk when reading the raw CSV.")
    parser.add_argument("--start-year", type=int, default=2018, help="Inclusive start year filter.")
    parser.add_argument("--end-year", type=int, default=2024, help="Inclusive end year filter.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not RAW_INCIDENTS.exists():
        raise FileNotFoundError(f"Missing {RAW_INCIDENTS}. Run scripts/download_data.py --dataset police_incidents first.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    crimes_csv = OUTPUT_DIR / "crimes_clean.csv"
    if crimes_csv.exists():
        crimes_csv.unlink()

    division_frames: list[pd.DataFrame] = []
    monthly_frames: list[pd.DataFrame] = []
    category_frames: list[pd.DataFrame] = []
    offense_frames: list[pd.DataFrame] = []
    zip_frames: list[pd.DataFrame] = []

    with sqlite3.connect(DB_PATH) as connection:
        connection.execute("DROP TABLE IF EXISTS crimes")

        wrote_chunk = False
        total_clean_rows = 0
        reader = pd.read_csv(
            RAW_INCIDENTS,
            usecols=USECOLS,
            dtype=str,
            chunksize=args.chunk_size,
            low_memory=False,
        )

        for chunk_number, raw_chunk in enumerate(reader, start=1):
            cleaned = build_clean_chunk(raw_chunk, args.start_year, args.end_year)
            if cleaned.empty:
                continue

            cleaned.to_csv(crimes_csv, mode="a", index=False, header=not wrote_chunk)
            cleaned.to_sql("crimes", connection, if_exists="append" if wrote_chunk else "replace", index=False)
            wrote_chunk = True
            total_clean_rows += len(cleaned)

            division_stats, monthly, category_year, offense_year, zip_stats = aggregate_clean_chunk(cleaned)
            division_frames.append(division_stats)
            monthly_frames.append(monthly)
            category_frames.append(category_year)
            offense_frames.append(offense_year)
            zip_frames.append(zip_stats)

            if chunk_number % 5 == 0:
                print(f"Processed chunk {chunk_number:,} | clean rows so far: {total_clean_rows:,}")

        division_stats = combine_grouped(
            division_frames,
            ["division", "year"],
            ["total_crimes", "violent_crimes", "property_crimes", "nighttime_crimes"],
        )
        division_stats = add_risk_scores(division_stats).sort_values(["year", "total_crimes"], ascending=[True, False])

        monthly_trends = combine_grouped(
            monthly_frames,
            ["division", "year", "month", "month_year"],
            ["incident_count"],
        ).sort_values(["month_year", "division"])

        crime_category_year = combine_grouped(
            category_frames,
            ["year", "crime_category"],
            ["incident_count"],
        ).sort_values(["year", "crime_category"])

        offense_year_counts = combine_grouped(
            offense_frames,
            ["crime_category", "offense_type", "year"],
            ["incident_count"],
        ).sort_values(["year", "incident_count"], ascending=[True, False])

        zip_crime_stats = combine_grouped(
            zip_frames,
            ["zip_code", "year"],
            ["total_crimes", "violent_crimes", "property_crimes", "nighttime_crimes"],
        ).sort_values(["year", "total_crimes"], ascending=[True, False])
        zip_crime_stats = add_risk_scores(zip_crime_stats)

        division_trends = build_division_trends(division_stats)

        write_csv(division_stats, OUTPUT_DIR / "division_stats.csv")
        write_csv(monthly_trends, OUTPUT_DIR / "monthly_trends.csv")
        write_csv(crime_category_year, OUTPUT_DIR / "crime_category_year.csv")
        write_csv(offense_year_counts, OUTPUT_DIR / "offense_year_counts.csv")
        write_csv(zip_crime_stats, OUTPUT_DIR / "zip_crime_stats.csv")
        write_csv(division_trends, OUTPUT_DIR / "division_trends.csv")

        division_stats.to_sql("division_stats", connection, if_exists="replace", index=False)
        monthly_trends.to_sql("monthly_trends", connection, if_exists="replace", index=False)
        crime_category_year.to_sql("crime_category_year", connection, if_exists="replace", index=False)
        offense_year_counts.to_sql("offense_year_counts", connection, if_exists="replace", index=False)
        zip_crime_stats.to_sql("zip_crime_stats", connection, if_exists="replace", index=False)
        division_trends.to_sql("division_trends", connection, if_exists="replace", index=False)
        create_indexes(connection)

    print(f"Saved {crimes_csv.relative_to(ROOT)} with {total_clean_rows:,} rows")
    print(f"Saved {(OUTPUT_DIR / 'division_stats.csv').relative_to(ROOT)}")
    print(f"Saved {(OUTPUT_DIR / 'division_trends.csv').relative_to(ROOT)}")
    print(f"Saved {(OUTPUT_DIR / 'monthly_trends.csv').relative_to(ROOT)}")
    print(f"Saved {(OUTPUT_DIR / 'zip_crime_stats.csv').relative_to(ROOT)}")
    print(f"Updated {DB_PATH.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
