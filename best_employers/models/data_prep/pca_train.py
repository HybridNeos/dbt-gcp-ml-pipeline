import pyspark.pandas as ps
import numpy as np
from sklearn.decomposition import PCA

def model(dbt, session):
    num_components = dbt.config.get("num_components")
    whiten_config = dbt.config.get("whiten")
    print(num_components, whiten_config)
    
    normalized_data = dbt.ref("train_data").toPandas()
    normalized_data = normalized_data.drop(["RANK"], axis=1)
    print(normalized_data.head())

    pca = PCA(n_components=num_components)
    components = pca.fit_transform(normalized_data)
    explained_variances = np.cumsum(pca.explained_variance_ratio_)

    result = np.vstack((pca.explained_variance_ratio_, components))

    names = [f"component_{x}" for x in range(1, result.shape[1]+1)]
    output_df = ps.DataFrame(data=result, columns=names)
    print(output_df.head())

    return output_df