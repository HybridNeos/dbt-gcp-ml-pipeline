SELECT *
FROM {{ ref('normalized_data') }}
WHERE RANK*100 NOT IN (SELECT INDEX FROM {{ ref('train_test_indices') }})