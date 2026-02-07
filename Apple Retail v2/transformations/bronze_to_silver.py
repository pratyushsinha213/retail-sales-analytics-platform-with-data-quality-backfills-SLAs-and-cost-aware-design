"""
Bronze → Silver: clean, standardize, and validate all 5 entities.
Reads from Bronze (Parquet when local, Delta when AWS), writes to Silver in the same format.
Run from Apple Retail v2: python -m transformations.bronze_to_silver [--env dev]
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from utils.config_loader import get_config
from utils.path_utils import get_layer_path, get_project_root
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _read_layer(spark, config, layer_key, table_name, read_format):
    path = get_layer_path(config, layer_key, table_name)
    return spark.read.format(read_format).load(path)


def _write_layer(df, config, layer_key, table_name, write_format):
    path = get_layer_path(config, layer_key, table_name)
    df.write.format(write_format).mode("overwrite").save(path)
    print(f"Silver: {table_name} -> {path}")


def clean_category(spark, config, read_fmt):
    df = _read_layer(spark, config, "bronze_base", "category", read_fmt)
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    df = df.dropDuplicates(["category_id"]).filter(F.col("category_id").isNotNull())
    return df


def clean_products(spark, config, read_fmt):
    df = _read_layer(spark, config, "bronze_base", "products", read_fmt)
    df = (
        df.withColumnRenamed("Product_ID", "product_id")
        .withColumnRenamed("Product_Name", "product_name")
        .withColumnRenamed("Category_ID", "category_id")
        .withColumnRenamed("Launch_Date", "launch_date")
        .withColumnRenamed("Price", "price")
    )
    # Ensure launch_date is date (CSV may read as string)
    if str(df.schema["launch_date"].dataType) not in ("DateType", "TimestampType"):
        df = df.withColumn("launch_date", F.to_date(F.col("launch_date").cast("string"), "yyyy-MM-dd"))
    df = df.dropDuplicates(["product_id"]).filter(F.col("product_id").isNotNull())
    return df


def clean_sales(spark, config, read_fmt):
    df = _read_layer(spark, config, "bronze_base", "sales", read_fmt)
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    df = df.withColumn("sale_date", F.to_date(F.col("sale_date"), "dd-MM-yyyy"))
    df = df.filter(
        F.col("sale_id").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
    )
    return df


def clean_stores(spark, config, read_fmt):
    df = _read_layer(spark, config, "bronze_base", "stores", read_fmt)
    df = (
        df.withColumnRenamed("Store_ID", "store_id")
        .withColumnRenamed("Store_Name", "store_name")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Country", "country")
    )
    df = df.dropDuplicates(["store_id"]).filter(F.col("store_id").isNotNull())
    return df


def clean_warranty(spark, config, read_fmt):
    df = _read_layer(spark, config, "bronze_base", "warranty", read_fmt)
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    df = df.withColumn("claim_date", F.to_date(F.col("claim_date"), "yyyy-MM-dd"))
    df = df.filter(F.col("claim_id").isNotNull() & F.col("sale_id").isNotNull())
    return df


def main(env: str = "dev") -> None:
    config = get_config(env)
    if config.get("use_local_paths"):
        os.chdir(get_project_root())

    use_local = config.get("use_local_paths", False)
    read_format = write_format = "parquet" if use_local else "delta"

    if use_local:
        spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()
    else:
        spark = (
            SparkSession.builder.appName("bronze-to-silver")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

    tables = [
        ("category", clean_category),
        ("products", clean_products),
        ("sales", clean_sales),
        ("stores", clean_stores),
        ("warranty", clean_warranty),
    ]
    for table_name, cleaner in tables:
        df = cleaner(spark, config, read_format)
        _write_layer(df, config, "silver_base", table_name, write_format)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev", help="Config environment (dev|prod)")
    args = parser.parse_args()
    main(env=args.env)
