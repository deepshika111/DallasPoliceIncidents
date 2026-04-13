# Dallas Crime Intelligence

Dallas public safety analytics project combining:

- live `Active Calls` for operational monitoring in Streamlit
- historical `Police Incidents` for trend analysis and risk scoring
- ZIP-level U.S. Census demographics for socioeconomic context
- Python, SQL, R, Excel, and Power BI deliverables in one repo

## Final Scope

This project now includes:

- Python ingestion and transformation scripts in [`scripts/`](scripts)
- a Streamlit dashboard in [`dashboard/app.py`](dashboard/app.py)
- SQL analysis queries in [`sql/crime_analysis_queries.sql`](sql/crime_analysis_queries.sql)
- an R analysis script in [`r_analysis/statistical_analysis.R`](r_analysis/statistical_analysis.R)
- an Excel report generator in [`scripts/export_excel_report.py`](scripts/export_excel_report.py)
- Power BI starter assets in [`powerbi/`](powerbi)

## Key Results

Using the current processed snapshot:

- cleaned and modeled `949,276` Dallas police incident records
- covered `2018` through `2024` (`7` historical years)
- built division-level risk scores for the latest year, with `NORTHWEST` currently ranking highest at `61.85`
- latest division comparison shows `2` improving divisions, `0` worsening divisions, and `6` stable divisions
- merged crime with Census ZIP demographics into `640` ZIP-year rows
- ZIP demographics summary found strong positive relationships between incidents per 1,000 residents and unemployment rate (`0.702`) and poverty rate (`0.677`)
- the ZIP-level linear regression summary currently has `R² = 0.651`

Live Active Calls are refreshable and time-sensitive. The most recent saved snapshot in this workspace contains `73` active calls.

## Data Sources

- Dallas OpenData `Active Calls`
- Dallas OpenData `Police Incidents`
- Dallas ArcGIS `Police Beats`
- U.S. Census ACS 2024 5-year API for ZIP demographics

Dataset configuration lives in [`config/datasets.json`](config/datasets.json).

## Repo Layout

```text
DallasPoliceIncidents/
├── config/
├── dashboard/
├── data/
│   ├── raw/
│   └── clean/
├── excel/
├── notebooks/
├── outputs/
├── powerbi/
├── r_analysis/
├── scripts/
└── sql/
```

## Run Locally

1. Download source data:

   ```bash
   python3 scripts/download_data.py --dataset active_calls --dataset police_beats --dataset police_incidents --dataset zip_demographics_acs
   ```

2. Build the beats reference for live mapping:

   ```bash
   python3 scripts/prepare_beats_reference.py
   ```

3. Process historical incidents and create SQLite outputs:

   ```bash
   python3 scripts/process_historical.py
   ```

4. Merge ZIP-level Census demographics:

   ```bash
   python3 scripts/process_zip_demographics.py
   ```

5. Refresh the live Active Calls snapshot:

   ```bash
   python3 scripts/fetch_active_calls.py
   ```

6. Generate charts and Excel outputs:

   ```bash
   python3 scripts/generate_eda_charts.py
   python3 scripts/export_excel_report.py
   ```

7. Launch the Streamlit dashboard:

   ```bash
   python3 -m streamlit run dashboard/app.py
   ```

## What Gets Produced

Main generated outputs include:

- `data/clean/crimes_clean.csv`
- `data/clean/division_stats.csv`
- `data/clean/division_trends.csv`
- `data/clean/monthly_trends.csv`
- `data/clean/zip_crime_stats.csv`
- `data/clean/zip_demographics.csv`
- `data/clean/zip_crime_demographics.csv`
- `data/clean/active_calls_latest.csv`
- `dallas_crime.db`
- `excel/dallas_crime_report.xlsx`
- charts in `outputs/`

## Dashboard Views

The Streamlit app includes:

- `Live Operations` with Active Calls KPIs and beat map
- `Historical Trends` with monthly division trends and risk scores
- `ZIP Demographics Context` with poverty vs incidents and top combined-risk ZIPs

## Notes for GitHub

- Large raw files, clean files, SQLite databases, and generated outputs are intentionally gitignored.
- That keeps the repo lightweight, but a fresh clone should rerun the pipeline commands above to regenerate local artifacts.
- If you want charts visible directly on GitHub, you can selectively stop ignoring specific files in `outputs/` and commit them.

## Limitations

- The historical incidents dataset is large, so processing is chunked.
- The sampled Dallas Active Calls endpoint does not expose coordinates, so the live map uses police beat centroids derived from the official beats geometry.
- Some ZIP codes in crime data are special-use ZIPs and may not have clean Census demographic matches.
- `Rscript` was not available in this shell during development, so the R script is included but should be validated in a local R environment.
