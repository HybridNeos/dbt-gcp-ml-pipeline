WITH data_maxes AS (
    SELECT
        count(*) as NUM_ROWS,
        MAX(CONTINENT_RANK) as NUM_CONTINENTS,
        MAX(NUM_INDUSTRIES) as MAX_INDUSTRIES,
        MAX(NUM_EMPLOYEES) as MAX_EMPLOYEES,
        MAX(NUM_EMPLOYEES_BIN) as MAX_EMPLOYEES_BIN
    FROM
        {{ ref("feature_engineering") }}
)
SELECT
    RANK              / (SELECT NUM_ROWS from data_maxes) as RANK,
    -- Actually PERCENT_IN_CONTINENT but I'll keep names consistent for downstream consumption
    NUM_IN_CONTINENT  / (SELECT NUM_ROWS from data_maxes) as NUM_IN_CONTINENT,
    CONTINENT_RANK    / (SELECT NUM_CONTINENTS from data_maxes) as CONTINENT_RANK,
    NUM_INDUSTRIES    / (SELECT MAX_INDUSTRIES from data_maxes) as NUM_INDUSTRIES,
    NUM_EMPLOYEES     / (SELECT MAX_EMPLOYEES from data_maxes)  as NUM_EMPLOYEES,
    NUM_EMPLOYEES_BIN / (SELECT MAX_EMPLOYEES_BIN from data_maxes) as NUM_EMPLOYEES_BIN
FROM
    {{ ref("feature_engineering") }}