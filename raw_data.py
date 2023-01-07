import pyspark.pandas as ps

def model(dbt, session):

    gsutil_name = dbt.config.get("gsutil_name")
    print(gsutil_name)
    df = ps.read_csv(gsutil_name)
    print(df.head())

    return df