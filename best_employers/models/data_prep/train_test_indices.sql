{{ config(materialized='view') }}

WITH training_indices AS (
  SELECT RANK, RAND() as x
  FROM {{ ref('feature_engineering' )}}
  ORDER BY x
  LIMIT 80
)
SELECT RANK as INDEX
FROM training_indices 