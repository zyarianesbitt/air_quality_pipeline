-- models/air_quality_summary.sql

WITH source_data AS (
    SELECT
        date_observed,
        city,
        state,
        pollutant,
        aqi,
        aqi_category,
        latitude,
        longitude
    FROM air_quality
    WHERE aqi IS NOT NULL
),

summary AS (
    SELECT
        date_observed,
        city,
        state,
        COUNT(*)                    AS total_readings,
        AVG(aqi)                    AS avg_aqi,
        MAX(aqi)                    AS max_aqi,
        MIN(aqi)                    AS min_aqi,
        STRING_AGG(pollutant, ', ') AS pollutants_measured,

        CASE
            WHEN MAX(aqi) <= 50  THEN 'Good'
            WHEN MAX(aqi) <= 100 THEN 'Moderate'
            WHEN MAX(aqi) <= 150 THEN 'Unhealthy for Sensitive Groups'
            WHEN MAX(aqi) > 150  THEN 'Unhealthy'
        END                         AS risk_level

    FROM source_data
    GROUP BY date_observed, city, state, latitude, longitude
)

SELECT * FROM summary
ORDER BY date_observed DESC, max_aqi DESC