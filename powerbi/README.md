# Power BI Starter Kit

This folder holds the assets that make the project Power BI-ready even though a `.pbix` file is not generated in this environment.

## Import These Files

- `../dallas_crime.db`
- `../data/clean/division_stats.csv`
- `../data/clean/division_trends.csv`
- `../data/clean/monthly_trends.csv`
- `../data/clean/crime_category_year.csv`
- `../data/clean/zip_crime_stats.csv`
- `../data/clean/zip_demographics.csv`
- `../data/clean/zip_crime_demographics.csv`
- `../data/clean/active_calls_latest.csv`
- `../data/clean/active_calls_by_beat.csv`
- `../data/clean/police_beats_reference.csv`

## Recommended Visuals

- Live Active Calls beat map:
  use `active_calls_by_beat.csv` with `centroid_lat` and `centroid_lon`
- Historical trend line:
  use `monthly_trends.csv`
- Risk score bar chart:
  use `division_stats.csv` filtered to the latest year
- Trend table:
  use `division_trends.csv`
- Crime category mix:
  use `crime_category_year.csv`
- ZIP demographics scatter:
  use `zip_crime_demographics.csv` with `poverty_rate_pct`, `incidents_per_1000`, and `combined_risk_score`

## Theme + Measures

- Apply `dallas_crime_theme.json` as the report theme.
- Use `measures.dax` as the starter measure library.

## Refresh Pattern

1. Run `python3 scripts/process_historical.py`
2. Run `python3 scripts/process_zip_demographics.py`
3. Run `python3 scripts/fetch_active_calls.py`
4. Refresh the Power BI dataset
