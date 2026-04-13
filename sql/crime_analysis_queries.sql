-- Historical analysis: total crimes by division and year
SELECT
    division,
    year,
    COUNT(*) AS total_crimes,
    SUM(CASE WHEN crime_category = 'Violent' THEN 1 ELSE 0 END) AS violent_crimes,
    SUM(CASE WHEN crime_category = 'Property' THEN 1 ELSE 0 END) AS property_crimes,
    ROUND(
        100.0 * SUM(CASE WHEN crime_category = 'Violent' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS violent_pct
FROM crimes
GROUP BY division, year
ORDER BY division, year;

-- Rank divisions by crime count within each year
SELECT
    division,
    year,
    total_crimes,
    RANK() OVER (PARTITION BY year ORDER BY total_crimes DESC) AS safety_rank,
    LAG(total_crimes) OVER (PARTITION BY division ORDER BY year) AS prev_year_crimes,
    total_crimes - LAG(total_crimes) OVER (PARTITION BY division ORDER BY year) AS year_over_year_change
FROM division_stats
ORDER BY year DESC, total_crimes DESC;

-- Improving vs worsening divisions
WITH crime_changes AS (
    SELECT
        division,
        year,
        total_crimes,
        LAG(total_crimes, 2) OVER (PARTITION BY division ORDER BY year) AS crimes_2_years_ago
    FROM division_stats
),
trend_analysis AS (
    SELECT
        division,
        year,
        total_crimes,
        crimes_2_years_ago,
        ROUND(100.0 * (total_crimes - crimes_2_years_ago) / crimes_2_years_ago, 2) AS pct_change,
        CASE
            WHEN total_crimes > crimes_2_years_ago * 1.05 THEN 'Worsening'
            WHEN total_crimes < crimes_2_years_ago * 0.95 THEN 'Improving'
            ELSE 'Stable'
        END AS trend
    FROM crime_changes
    WHERE crimes_2_years_ago IS NOT NULL
)
SELECT *
FROM trend_analysis
WHERE year = (SELECT MAX(year) FROM division_stats)
ORDER BY pct_change DESC;

-- Most dangerous times by division
SELECT
    division,
    day_of_week,
    time_of_day,
    COUNT(*) AS incidents,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY division),
        2
    ) AS pct_of_division_crimes
FROM crimes
GROUP BY division, day_of_week, time_of_day
ORDER BY division, incidents DESC;

-- Crime type trends over time
SELECT
    offense_type,
    year,
    COUNT(*) AS count,
    COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY offense_type ORDER BY year) AS change_from_prior_year,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY year),
        2
    ) AS share_of_year
FROM crimes
GROUP BY offense_type, year
ORDER BY year, count DESC;

-- Live operations: active calls by division
SELECT
    division,
    COUNT(*) AS active_calls,
    SUM(CASE WHEN priority = 1 THEN 1 ELSE 0 END) AS priority_1_calls
FROM active_calls_latest
GROUP BY division
ORDER BY active_calls DESC;

-- Live operations: busiest beats right now
SELECT
    beat,
    division,
    active_calls,
    priority_1_calls
FROM active_calls_by_beat
ORDER BY active_calls DESC, beat;

-- ZIP code crime rates with demographics
SELECT
    zip_code,
    year,
    total_crimes,
    ROUND(incidents_per_1000, 2) AS incidents_per_1000,
    ROUND(poverty_rate_pct, 2) AS poverty_rate_pct,
    ROUND(unemployment_rate_pct, 2) AS unemployment_rate_pct,
    ROUND(bachelors_plus_rate_pct, 2) AS bachelors_plus_rate_pct,
    ROUND(combined_risk_score, 2) AS combined_risk_score
FROM zip_crime_demographics
ORDER BY year DESC, combined_risk_score DESC;

-- High-poverty ZIP codes with crime context
SELECT
    zip_code,
    total_population,
    median_household_income,
    ROUND(poverty_rate_pct, 2) AS poverty_rate_pct,
    ROUND(incidents_per_1000, 2) AS incidents_per_1000,
    ROUND(violent_incidents_per_1000, 2) AS violent_incidents_per_1000
FROM zip_crime_demographics
WHERE year = (SELECT MAX(year) FROM zip_crime_demographics)
ORDER BY poverty_rate_pct DESC, incidents_per_1000 DESC;
