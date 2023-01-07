SELECT
    fe_data.*
FROM
    {{ ref('normalized_data') }} fe_data
    INNER JOIN {{ ref('train_test_indices') }} indices
        ON fe_data.RANK*100 = indices.INDEX
