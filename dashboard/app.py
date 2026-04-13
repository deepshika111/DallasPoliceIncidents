#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd
import plotly.express as px
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "dallas_crime.db"
BEATS_REFERENCE = ROOT / "data" / "clean" / "police_beats_reference.csv"
ACTIVE_CALLS_FALLBACK = ROOT / "data" / "clean" / "active_calls_latest.csv"
ACTIVE_CALLS_URL = "https://www.dallasopendata.com/resource/9fxf-t2tr.json?$limit=50000"


def build_headers() -> dict[str, str]:
    headers = {"User-Agent": "dallas-crime-intelligence/0.1"}
    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token
    return headers


def normalize_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_live_active_calls() -> pd.DataFrame:
    try:
        request = Request(ACTIVE_CALLS_URL, headers=build_headers())
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
        calls = pd.DataFrame(payload)
    except Exception:
        if ACTIVE_CALLS_FALLBACK.exists():
            calls = pd.read_csv(ACTIVE_CALLS_FALLBACK)
        else:
            return pd.DataFrame()

    if calls.empty:
        return calls

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
    calls["priority"] = pd.to_numeric(calls["priority"], errors="coerce")
    parsed_dates = pd.to_datetime(calls["date"], errors="coerce")
    calls["call_timestamp"] = pd.to_datetime(parsed_dates.dt.strftime("%Y-%m-%d").fillna("") + " " + calls["time"].fillna(""), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return calls


@st.cache_data(show_spinner=False)
def load_beats_reference() -> pd.DataFrame:
    if not BEATS_REFERENCE.exists():
        return pd.DataFrame()
    return pd.read_csv(BEATS_REFERENCE, dtype={"beat": "string", "sector": "string", "division": "string"})


@st.cache_data(show_spinner=False)
def load_historical_tables() -> dict[str, pd.DataFrame]:
    if not DB_PATH.exists():
        return {}

    tables = {}
    with sqlite3.connect(DB_PATH) as connection:
        for table in ["division_stats", "division_trends", "monthly_trends", "crime_category_year", "zip_crime_demographics"]:
            try:
                tables[table] = pd.read_sql_query(f"SELECT * FROM {table}", connection)
            except Exception:
                tables[table] = pd.DataFrame()
    return tables


def build_live_beat_map(calls: pd.DataFrame, beats: pd.DataFrame):
    if calls.empty or beats.empty:
        return None

    live_by_beat = (
        calls.assign(priority_1_flag=(calls["priority"] == 1).astype(int))
        .groupby("beat", as_index=False)
        .agg(
            active_calls=("incident_number", "count"),
            priority_1_calls=("priority_1_flag", "sum"),
        )
        .merge(beats, on="beat", how="left")
        .dropna(subset=["centroid_lat", "centroid_lon"])
    )

    if live_by_beat.empty:
        return None

    figure = px.scatter_mapbox(
        live_by_beat,
        lat="centroid_lat",
        lon="centroid_lon",
        size="active_calls",
        color="division",
        hover_name="beat",
        hover_data={"active_calls": True, "priority_1_calls": True, "sector": True, "centroid_lat": False, "centroid_lon": False},
        zoom=9.2,
        height=520,
    )
    figure.update_layout(mapbox_style="carto-positron", margin={"l": 0, "r": 0, "t": 0, "b": 0})
    return figure


def main() -> None:
    st.set_page_config(page_title="Dallas Crime Intelligence", layout="wide")

    st.title("Dallas Crime Intelligence")
    st.caption("Live Active Calls for operations, historical Police Incidents for trends and risk scoring.")

    if st.sidebar.button("Refresh Live Feed"):
        fetch_live_active_calls.clear()

    historical = load_historical_tables()
    division_stats = historical.get("division_stats", pd.DataFrame())
    division_trends = historical.get("division_trends", pd.DataFrame())
    monthly_trends = historical.get("monthly_trends", pd.DataFrame())
    crime_category_year = historical.get("crime_category_year", pd.DataFrame())
    zip_crime_demographics = historical.get("zip_crime_demographics", pd.DataFrame())

    live_calls = fetch_live_active_calls()
    beats = load_beats_reference()

    if division_stats.empty:
        st.warning("Historical outputs are missing. Run `python3 scripts/process_historical.py` first.")
    if beats.empty:
        st.warning("Beat reference is missing. Run `python3 scripts/prepare_beats_reference.py` first.")

    year_min = int(division_stats["year"].min()) if not division_stats.empty else 2018
    year_max = int(division_stats["year"].max()) if not division_stats.empty else 2024
    year_range = st.sidebar.slider("Historical Year Range", min_value=year_min, max_value=year_max, value=(year_min, year_max))

    divisions = sorted(division_stats["division"].dropna().unique().tolist()) if not division_stats.empty else []
    selected_divisions = st.sidebar.multiselect("Divisions", options=divisions, default=divisions)

    if not division_stats.empty:
        division_stats = division_stats.loc[
            division_stats["year"].between(year_range[0], year_range[1])
            & (division_stats["division"].isin(selected_divisions) if selected_divisions else True)
        ].copy()

    if not monthly_trends.empty:
        monthly_trends["month_start"] = pd.to_datetime(monthly_trends["month_year"] + "-01")
        monthly_trends = monthly_trends.loc[
            monthly_trends["year"].between(year_range[0], year_range[1])
            & (monthly_trends["division"].isin(selected_divisions) if selected_divisions else True)
        ].copy()

    if not zip_crime_demographics.empty:
        zip_crime_demographics = zip_crime_demographics.loc[
            zip_crime_demographics["year"].between(year_range[0], year_range[1])
        ].copy()

    st.subheader("Live Operations")
    live_col1, live_col2, live_col3, live_col4 = st.columns(4)
    if live_calls.empty:
        live_col1.metric("Active Calls", "N/A")
        live_col2.metric("Priority 1 Calls", "N/A")
        live_col3.metric("Busiest Division", "N/A")
        live_col4.metric("Active Beats", "N/A")
        st.info("Live feed unavailable right now. The dashboard will fall back to the last saved snapshot if one exists.")
    else:
        live_division_counts = live_calls.groupby("division").size().sort_values(ascending=False)
        busiest_division = live_division_counts.index[0] if not live_division_counts.empty else "N/A"
        priority_1_calls = int((live_calls["priority"] == 1).sum())
        active_beats = int(live_calls["beat"].nunique())

        live_col1.metric("Active Calls", f"{len(live_calls):,}")
        live_col2.metric("Priority 1 Calls", f"{priority_1_calls:,}")
        live_col3.metric("Busiest Division", busiest_division)
        live_col4.metric("Active Beats", f"{active_beats:,}")

        map_col, table_col = st.columns([1.7, 1])
        with map_col:
            figure = build_live_beat_map(live_calls, beats)
            if figure is not None:
                st.plotly_chart(figure, use_container_width=True)
            else:
                st.info("Live map is waiting on beat reference data.")
        with table_col:
            st.markdown("**Top Live Call Types**")
            call_types = live_calls.groupby("nature_of_call", as_index=False).size().rename(columns={"size": "active_calls"})
            st.dataframe(call_types.sort_values("active_calls", ascending=False).head(10), use_container_width=True, hide_index=True)
            st.markdown("**Current Calls Snapshot**")
            st.dataframe(
                live_calls[["incident_number", "division", "nature_of_call", "priority", "beat", "status"]].head(20),
                use_container_width=True,
                hide_index=True,
            )

    st.subheader("Historical Trends")
    hist_col1, hist_col2 = st.columns(2)

    with hist_col1:
        if monthly_trends.empty:
            st.info("Monthly historical trends will appear after you process the incidents file.")
        else:
            trend_figure = px.line(
                monthly_trends,
                x="month_start",
                y="incident_count",
                color="division",
                markers=True,
                title="Monthly Incident Trend by Division",
            )
            trend_figure.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0})
            st.plotly_chart(trend_figure, use_container_width=True)

    with hist_col2:
        if division_stats.empty:
            st.info("Risk scores will appear after the historical pipeline finishes.")
        else:
            latest_year = int(division_stats["year"].max())
            latest_risk = division_stats.loc[division_stats["year"] == latest_year].sort_values("risk_score", ascending=False)
            risk_figure = px.bar(
                latest_risk,
                x="risk_score",
                y="division",
                orientation="h",
                color="risk_score",
                color_continuous_scale=["#2f7d32", "#f0c419", "#c62828"],
                title=f"Risk Scores by Division ({latest_year})",
            )
            risk_figure.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0}, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(risk_figure, use_container_width=True)

    mix_col, trend_table_col = st.columns([1.2, 1])
    with mix_col:
        if crime_category_year.empty:
            st.info("Crime category mix will appear after the historical pipeline finishes.")
        else:
            mix = crime_category_year.loc[crime_category_year["year"].between(year_range[0], year_range[1])].copy()
            mix_figure = px.bar(
                mix,
                x="year",
                y="incident_count",
                color="crime_category",
                title="Crime Category Mix by Year",
                barmode="stack",
            )
            mix_figure.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0})
            st.plotly_chart(mix_figure, use_container_width=True)

    with trend_table_col:
        st.markdown("**Division Trend Snapshot**")
        if division_trends.empty:
            st.info("Trend table will appear after the historical pipeline finishes.")
        else:
            trend_table = division_trends[["division", "trend_arrow", "trend_label", "crime_change_pct", "pct_change_since_baseline", "risk_score"]].copy()
            trend_table["crime_change_pct"] = trend_table["crime_change_pct"].round(2)
            trend_table["pct_change_since_baseline"] = trend_table["pct_change_since_baseline"].round(2)
            st.dataframe(trend_table, use_container_width=True, hide_index=True)

    st.subheader("ZIP Demographics Context")
    demo_col1, demo_col2 = st.columns([1.4, 1])
    with demo_col1:
        if zip_crime_demographics.empty:
            st.info("ZIP demographics will appear after you run `python3 scripts/process_zip_demographics.py`.")
        else:
            latest_zip_year = int(zip_crime_demographics["year"].max())
            latest_zip = zip_crime_demographics.loc[zip_crime_demographics["year"] == latest_zip_year].dropna(
                subset=["incidents_per_1000", "poverty_rate_pct", "total_population"]
            )
            if latest_zip.empty:
                st.info("ZIP demographics output exists, but there are no rows ready for the scatter chart yet.")
            else:
                zip_figure = px.scatter(
                    latest_zip,
                    x="poverty_rate_pct",
                    y="incidents_per_1000",
                    size="total_population",
                    color="combined_risk_score",
                    hover_name="zip_code",
                    hover_data={
                        "median_household_income": ":,.0f",
                        "unemployment_rate_pct": ":.2f",
                        "bachelors_plus_rate_pct": ":.2f",
                        "combined_risk_score": ":.2f",
                    },
                    color_continuous_scale=["#2f7d32", "#f0c419", "#c62828"],
                    title=f"ZIP Poverty Rate vs Incidents per 1,000 ({latest_zip_year})",
                )
                zip_figure.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0})
                st.plotly_chart(zip_figure, use_container_width=True)

    with demo_col2:
        st.markdown("**Highest Combined ZIP Risk**")
        if zip_crime_demographics.empty:
            st.info("Run the ZIP demographics processor to populate this view.")
        else:
            latest_zip_year = int(zip_crime_demographics["year"].max())
            top_zip = (
                zip_crime_demographics.loc[zip_crime_demographics["year"] == latest_zip_year, [
                    "zip_code",
                    "combined_risk_score",
                    "incidents_per_1000",
                    "poverty_rate_pct",
                    "unemployment_rate_pct",
                    "median_household_income",
                ]]
                .sort_values("combined_risk_score", ascending=False)
                .head(12)
                .copy()
            )
            numeric_cols = [
                "combined_risk_score",
                "incidents_per_1000",
                "poverty_rate_pct",
                "unemployment_rate_pct",
                "median_household_income",
            ]
            for col in numeric_cols:
                top_zip[col] = top_zip[col].round(2)
            st.dataframe(top_zip, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
