import yaml
from pyspark.sql import SparkSession

def load_config(env):
    with open(f"config/{env}.yaml") as f:
        return yaml.safe_load(f)

def main(env="dev"):
    config = load_config(env)

    spark = (
        SparkSession.builder
        .appName("bronze-to-silver")
        .getOrCreate()
    )

    bronze_df = spark.read.format("delta").load(
        f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net/{config['bronze_path']}"
    )

    # TODO: transformations + DQ
    silver_df = bronze_df

    silver_df.write.format("delta") \
        .mode("overwrite") \
        .save(
            f"abfss://{config['container']}@{config['storage_account']}.dfs.core.windows.net/{config['silver_path']}"
        )

if __name__ == "__main__":
    main()