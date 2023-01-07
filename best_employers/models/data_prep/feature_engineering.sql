WITH continent_ordering AS (
  SELECT
  map.CONTINENT,
  count(*) as NUM_IN_CONTINENT,
  -- This could be a one-hot encoded feature
  -- But, we'll make it ordinal by saying the continents with more employers rank higher  
  ROW_NUMBER() OVER (PARTITION BY '' ORDER BY count(*) desc) as CONTINENT_RANK
  FROM
    {{ ref('clean_data') }} clean_data
    LEFT JOIN {{ ref('country_to_continent') }} map
      ON clean_data.COUNTRY_TERRITORY = map.COUNTRY
  GROUP BY
    map.CONTINENT
)
SELECT
  clean_data.RANK,
  continent_ordering.NUM_IN_CONTINENT,
  continent_ordering.CONTINENT_RANK,
  GREATEST(1, LENGTH(clean_data.INDUSTRIES) - LENGTH(REPLACE(clean_data.INDUSTRIES, ',', ''))) as NUM_INDUSTRIES,
  clean_data.NUM_EMPLOYEES,
  CAST(CEIL(ROW_NUMBER() OVER (PARTITION BY '' ORDER BY clean_data.NUM_EMPLOYEES DESC) / 20) as INT64) as NUM_EMPLOYEES_BIN
FROM
  {{ ref('clean_data') }} clean_data
  INNER JOIN {{ ref('country_to_continent') }} map
    ON clean_data.COUNTRY_TERRITORY = map.COUNTRY
  INNER JOIN continent_ordering
    ON map.CONTINENT = continent_ordering.CONTINENT