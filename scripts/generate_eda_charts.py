#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".mplconfig"))

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


DATA_DIR = ROOT / "data" / "clean"
OUTPUT_DIR = ROOT / "outputs"


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    crimes_path = DATA_DIR / "crimes_clean.csv"
    division_stats_path = DATA_DIR / "division_stats.csv"
    category_year_path = DATA_DIR / "crime_category_year.csv"
    trends_path = DATA_DIR / "division_trends.csv"
    zip_path = DATA_DIR / "zip_crime_demographics.csv"

    missing = [path for path in [crimes_path, division_stats_path, category_year_path, trends_path] if not path.exists()]
    if missing:
        missing_text = ", ".join(str(path.relative_to(ROOT)) for path in missing)
        raise FileNotFoundError(f"Missing required clean output(s): {missing_text}. Run scripts/process_historical.py first.")

    crimes = pd.read_csv(crimes_path, usecols=["hour"])
    division_stats = pd.read_csv(division_stats_path)
    category_year = pd.read_csv(category_year_path)
    trends = pd.read_csv(trends_path)
    zip_demographics = pd.read_csv(zip_path) if zip_path.exists() else pd.DataFrame()
    return crimes, division_stats, category_year, trends, zip_demographics


def apply_theme() -> None:
    sns.set_theme(style="whitegrid", palette="deep")
    plt.rcParams["figure.facecolor"] = "#f8f6f1"
    plt.rcParams["axes.facecolor"] = "#fdfcf9"
    plt.rcParams["savefig.facecolor"] = "#f8f6f1"
    plt.rcParams["axes.edgecolor"] = "#d6d0c4"
    plt.rcParams["axes.titleweight"] = "bold"


def save_yearly_trend(division_stats: pd.DataFrame) -> None:
    yearly = division_stats.groupby("year", as_index=False)["total_crimes"].sum()

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.lineplot(data=yearly, x="year", y="total_crimes", marker="o", linewidth=2.5, color="#9f1d20", ax=ax)
    ax.set_title("Dallas Total Crime Incidents by Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Incident Count")
    ax.ticklabel_format(style="plain", axis="y")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "yearly_trend.png", dpi=180)
    plt.close(fig)


def save_division_heatmap(division_stats: pd.DataFrame) -> None:
    pivot = (
        division_stats.pivot(index="division", columns="year", values="total_crimes")
        .sort_index()
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(pivot, annot=True, fmt=".0f", cmap="Reds", linewidths=0.5, cbar_kws={"label": "Incident Count"}, ax=ax)
    ax.set_title("Crime Count by Division and Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Division")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "division_heatmap.png", dpi=180)
    plt.close(fig)


def save_hourly_pattern(crimes: pd.DataFrame) -> None:
    hourly = (
        crimes.dropna(subset=["hour"])
        .assign(hour=lambda df: df["hour"].astype(int))
        .groupby("hour", as_index=False)
        .size()
        .rename(columns={"size": "incident_count"})
    )
    hourly = hourly.set_index("hour").reindex(range(24), fill_value=0).reset_index()

    fig, ax = plt.subplots(figsize=(11, 5))
    sns.barplot(data=hourly, x="hour", y="incident_count", color="#3b6ea8", ax=ax)
    ax.set_title("Crime by Hour of Day")
    ax.set_xlabel("Hour")
    ax.set_ylabel("Incident Count")
    ax.ticklabel_format(style="plain", axis="y")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "hourly_pattern.png", dpi=180)
    plt.close(fig)


def save_crime_category_breakdown(category_year: pd.DataFrame) -> None:
    totals = (
        category_year.groupby("crime_category", as_index=False)["incident_count"].sum()
        .sort_values("incident_count", ascending=False)
    )
    color_map = {
        "Violent": "#b22222",
        "Property": "#d98c10",
        "Other": "#7a7a7a",
    }
    colors = [color_map.get(category, "#4c72b0") for category in totals["crime_category"]]

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(
        totals["incident_count"],
        labels=totals["crime_category"],
        autopct="%1.1f%%",
        startangle=90,
        colors=colors,
        wedgeprops={"edgecolor": "white", "linewidth": 1.2},
        textprops={"fontsize": 11},
    )
    ax.set_title("Crime Category Distribution (2018-2024)")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "crime_categories.png", dpi=180)
    plt.close(fig)


def save_risk_score_bars(division_stats: pd.DataFrame, trends: pd.DataFrame) -> None:
    latest_year = int(division_stats["year"].max())
    latest = division_stats.loc[division_stats["year"] == latest_year, ["division", "risk_score"]].copy()
    latest = latest.merge(trends[["division", "trend_label"]], on="division", how="left")
    latest = latest.sort_values("risk_score", ascending=True)

    color_map = {
        "Improving": "#2f7d32",
        "Stable": "#d99a00",
        "Worsening": "#b22222",
    }
    colors = latest["trend_label"].map(color_map).fillna("#4c72b0")

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(latest["division"], latest["risk_score"], color=colors)
    ax.set_title(f"Dallas Division Risk Scores ({latest_year})")
    ax.set_xlabel("Risk Score")
    ax.set_ylabel("Division")
    for value, division in zip(latest["risk_score"], latest["division"]):
        ax.text(value + 0.6, division, f"{value:.1f}", va="center", fontsize=9)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "risk_scores.png", dpi=180)
    plt.close(fig)


def save_zip_poverty_scatter(zip_demographics: pd.DataFrame) -> None:
    if zip_demographics.empty:
        return

    latest_year = int(zip_demographics["year"].max())
    latest = zip_demographics.loc[
        (zip_demographics["year"] == latest_year)
        & zip_demographics["incidents_per_1000"].notna()
        & zip_demographics["poverty_rate_pct"].notna()
        & (zip_demographics["total_population"] >= 1000)
    ].copy()
    if latest.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(
        latest["poverty_rate_pct"],
        latest["incidents_per_1000"],
        s=(latest["total_population"] / latest["total_population"].max()) * 900 + 40,
        c=latest["combined_risk_score"],
        cmap="YlOrRd",
        alpha=0.75,
        edgecolors="white",
        linewidths=0.7,
    )

    for _, row in latest.sort_values("combined_risk_score", ascending=False).head(8).iterrows():
        ax.annotate(row["zip_code"], (row["poverty_rate_pct"], row["incidents_per_1000"]), fontsize=8, xytext=(4, 4), textcoords="offset points")

    ax.set_title(f"ZIP Poverty Rate vs Incidents per 1,000 ({latest_year})")
    ax.set_xlabel("Poverty Rate (%)")
    ax.set_ylabel("Incidents per 1,000 Residents")
    colorbar = fig.colorbar(scatter, ax=ax)
    colorbar.set_label("Combined Risk Score")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "zip_poverty_vs_incidents.png", dpi=180)
    plt.close(fig)


def save_zip_combined_risk(zip_demographics: pd.DataFrame) -> None:
    if zip_demographics.empty:
        return

    latest_year = int(zip_demographics["year"].max())
    latest = zip_demographics.loc[
        (zip_demographics["year"] == latest_year)
        & zip_demographics["combined_risk_score"].notna()
        & (zip_demographics["total_population"] >= 1000)
    ].copy()
    if latest.empty:
        return

    top_zip = latest.sort_values("combined_risk_score", ascending=False).head(15).sort_values("combined_risk_score", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 6))
    bars = ax.barh(top_zip["zip_code"], top_zip["combined_risk_score"], color="#b22222")
    ax.set_title(f"Top ZIP Combined Risk Scores ({latest_year})")
    ax.set_xlabel("Combined Risk Score")
    ax.set_ylabel("ZIP Code")
    for bar, value in zip(bars, top_zip["combined_risk_score"]):
        ax.text(bar.get_width() + 0.6, bar.get_y() + bar.get_height() / 2, f"{value:.1f}", va="center", fontsize=9)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "zip_combined_risk.png", dpi=180)
    plt.close(fig)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    apply_theme()
    crimes, division_stats, category_year, trends, zip_demographics = load_inputs()

    save_yearly_trend(division_stats)
    save_division_heatmap(division_stats)
    save_hourly_pattern(crimes)
    save_crime_category_breakdown(category_year)
    save_risk_score_bars(division_stats, trends)
    save_zip_poverty_scatter(zip_demographics)
    save_zip_combined_risk(zip_demographics)

    print("Saved outputs/yearly_trend.png")
    print("Saved outputs/division_heatmap.png")
    print("Saved outputs/hourly_pattern.png")
    print("Saved outputs/crime_categories.png")
    print("Saved outputs/risk_scores.png")
    if not zip_demographics.empty:
        print("Saved outputs/zip_poverty_vs_incidents.png")
        print("Saved outputs/zip_combined_risk.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
