#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW_BEATS = ROOT / "data" / "raw" / "Police_Beats.geojson"
OUTPUT_CSV = ROOT / "data" / "clean" / "police_beats_reference.csv"
DB_PATH = ROOT / "dallas_crime.db"


def iter_points(coordinates: list) -> Iterable[tuple[float, float]]:
    if not coordinates:
        return

    first = coordinates[0]
    if isinstance(first, (float, int)) and len(coordinates) >= 2:
        yield float(coordinates[0]), float(coordinates[1])
        return

    for item in coordinates:
        yield from iter_points(item)


def centroid_from_geometry(geometry: dict) -> tuple[float | None, float | None]:
    if not geometry:
        return None, None

    points = list(iter_points(geometry.get("coordinates", [])))
    if not points:
        return None, None

    longitudes = [point[0] for point in points]
    latitudes = [point[1] for point in points]
    return sum(latitudes) / len(latitudes), sum(longitudes) / len(longitudes)


def build_beats_reference_frame() -> pd.DataFrame:
    with RAW_BEATS.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    rows: list[dict[str, object]] = []
    for feature in payload.get("features", []):
        properties = feature.get("properties", {})
        centroid_lat, centroid_lon = centroid_from_geometry(feature.get("geometry", {}))
        rows.append(
            {
                "beat": str(properties.get("BEAT", "")).strip(),
                "sector": str(properties.get("SECTOR", "")).strip(),
                "division": str(properties.get("DIVISION", "")).strip().upper(),
                "centroid_lat": centroid_lat,
                "centroid_lon": centroid_lon,
            }
        )

    frame = pd.DataFrame(rows).drop_duplicates(subset=["beat"]).sort_values("beat").reset_index(drop=True)
    return frame


def main() -> int:
    if not RAW_BEATS.exists():
        raise FileNotFoundError(f"Missing {RAW_BEATS}. Run scripts/download_data.py --dataset police_beats first.")

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    beats = build_beats_reference_frame()
    beats.to_csv(OUTPUT_CSV, index=False)

    with sqlite3.connect(DB_PATH) as connection:
        beats.to_sql("police_beats_reference", connection, if_exists="replace", index=False)

    print(f"Saved {OUTPUT_CSV.relative_to(ROOT)} with {len(beats)} beats")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
