WITH data_maxes AS (
    SELECT
        count(*) as NUM_ROWS,
        MAX(CONTINENT_RANK) as NUM_CONTINENTS,
        MAX(NUM_INDUSTRIES) as MAX_INDUSTRIES,
        --MIN(NUM_INDUSTRIES) as MIN_INDUSTRIES,
        MAX(NUM_EMPLOYEES) as MAX_EMPLOYEES,
        --MIN(NUM_EMPLOYEES) as MIN_EMPLOYEES,
        MAX(NUM_EMPLOYEES_BIN) as NUM_BINs
    FROM
        {{ ref("feature_engineering") }}
)
SELECT
    RANK              / (SELECT NUM_ROWS from data_maxes) as RANK,
    -- Name change here prevents easy swap of normalized and non-normalized data
    NUM_IN_CONTINENT  / (SELECT NUM_ROWS from data_maxes) as PERCENT_IN_CONTINENT,
    CONTINENT_RANK    / (SELECT NUM_CONTINENTS from data_maxes) as CONTINENT_RANK,
    NUM_INDUSTRIES    / (SELECT MAX_INDUSTRIES from data_maxes) as NUM_INDUSTRIES,
    NUM_EMPLOYEES     / (SELECT MAX_EMPLOYEES from data_maxes)  as NUM_EMPLOYEES,
    NUM_EMPLOYEES_BIN / (SELECT NUM_BINS from data_maxes) as NUM_EMPLOYEES_BIN
FROM
    {{ ref("feature_engineering") }}