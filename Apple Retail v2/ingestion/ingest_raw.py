"""
Ingest raw CSVs from raw-data-source into Bronze layer.
- Local (use_local_paths): writes Parquet (no Delta JAR needed; avoids Scala classpath issues with pip PySpark).
- AWS/Glue: writes Delta (Glue has Delta on classpath).
Run from Apple Retail v2: python -m ingestion.ingest_raw [--env dev]
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from utils.config_loader import get_config
from utils.path_utils import get_layer_path, get_raw_csv_path, get_project_root

RAW_FILES = {
    "category": "category.csv",
    "products": "products.csv",
    "sales": "sales.csv",
    "stores": "stores.csv",
    "warranty": "warranty.csv",
}


def main(env: str = "dev") -> None:
    config = get_config(env)
    if config.get("use_local_paths"):
        os.chdir(get_project_root())

    raw_path = config.get("raw_data_path")
    if config.get("use_local_paths") and not raw_path:
        raise ValueError("config.raw_data_path is required for local ingestion (e.g. ./raw-data-source)")

    from pyspark.sql import SparkSession

    use_local = config.get("use_local_paths", False)
    if use_local:
        # Parquet only – works with plain PySpark, no Delta JAR (avoids Scala classpath issues).
        spark = SparkSession.builder.appName("ingest-bronze").getOrCreate()
        write_format = "parquet"
    else:
        # Delta for AWS/Glue (Glue runtime has Delta on classpath).
        spark = (
            SparkSession.builder.appName("ingest-bronze")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        write_format = "delta"

    for table_name, filename in RAW_FILES.items():
        csv_path = get_raw_csv_path(config, filename)
        if csv_path.startswith("s3://"):
            pass
        elif not os.path.exists(csv_path):
            alt = os.path.join(get_project_root(), raw_path or ".", filename) if raw_path else csv_path
            if not os.path.exists(alt):
                print(f"Warning: {csv_path} not found, skipping {table_name}")
                continue
            csv_path = alt

        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        bronze_path = get_layer_path(config, "bronze_base", table_name)
        df.write.format(write_format).mode("overwrite").save(bronze_path)
        print(f"Bronze: {table_name} -> {bronze_path} ({write_format})")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev", help="Config environment (dev|prod)")
    args = parser.parse_args()
    main(env=args.env)
