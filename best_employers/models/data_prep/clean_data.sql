SELECT
    RANK,
    NAME,
    INDUSTRIES,
    CAST(REPLACE(EMPLOYEES, ',', '') AS INT64) as NUM_EMPLOYEES,
    CASE
        WHEN COUNTRY_TERRITORY = 'South Kore'
        THEN 'SOUTH KOREA'
        ELSE UPPER(COUNTRY_TERRITORY)
    END AS COUNTRY_TERRITORY
FROM
    {{ source("best_employers", "raw_data") }}