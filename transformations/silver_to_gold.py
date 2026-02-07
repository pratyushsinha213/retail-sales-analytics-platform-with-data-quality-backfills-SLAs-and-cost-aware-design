"""
Silver → Gold: build star schema (fact_sales + dim_category, dim_product, dim_store).
Reads from Silver (Parquet when local, Delta when AWS), writes Gold in the same format.
Run from Apple Retail v2: python -m transformations.silver_to_gold [--env dev]
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


def _read_silver(spark, config, table_name, read_format):
    path = get_layer_path(config, "silver_base", table_name)
    return spark.read.format(read_format).load(path)


def _write_gold(df, config, table_name, write_format):
    path = get_layer_path(config, "gold_base", table_name)
    df.write.format(write_format).mode("overwrite").save(path)
    print(f"Gold: {table_name} -> {path}")


def main(env: str = "dev") -> None:
    config = get_config(env)
    if config.get("use_local_paths"):
        os.chdir(get_project_root())

    use_local = config.get("use_local_paths", False)
    read_format = write_format = "parquet" if use_local else "delta"

    if use_local:
        spark = SparkSession.builder.appName("silver-to-gold").getOrCreate()
    else:
        spark = (
            SparkSession.builder.appName("silver-to-gold")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

    silver_category = _read_silver(spark, config, "category", read_format)
    silver_products = _read_silver(spark, config, "products", read_format)
    silver_sales = _read_silver(spark, config, "sales", read_format)
    silver_stores = _read_silver(spark, config, "stores", read_format)
    silver_warranty = _read_silver(spark, config, "warranty", read_format)

    # Dims with surrogate keys
    dim_category = silver_category.withColumn("category_key", F.monotonically_increasing_id()).select(
        "category_key", "category_id", "category_name"
    )
    dim_product = silver_products.withColumn("product_key", F.monotonically_increasing_id()).select(
        "product_key", "product_id", "product_name", "category_id", "launch_date", "price"
    )
    dim_store = silver_stores.withColumn("store_key", F.monotonically_increasing_id()).select(
        "store_key", "store_id", "store_name", "city", "country"
    )

    # Fact: sales + product (price, category_id) + store_key + category_key; warranty → has_claim; revenue = quantity * price
    sales_product = silver_sales.join(
        dim_product.select("product_id", "product_key", "price", "category_id"),
        on="product_id",
        how="left",
    )
    sales_store = sales_product.join(
        dim_store.select("store_id", "store_key"),
        on="store_id",
        how="left",
    )
    sales_category = sales_store.join(
        dim_category.select("category_id", "category_key"),
        on="category_id",
        how="left",
    )
    claims = (
        silver_warranty.groupBy("sale_id")
        .agg(F.count("*").alias("_n"))
        .withColumn("has_claim", F.when(F.col("_n") > 0, 1).otherwise(0))
        .select("sale_id", "has_claim")
    )
    fact_sales = (
        sales_category.join(claims, on="sale_id", how="left")
        .withColumn("has_claim", F.coalesce(F.col("has_claim"), F.lit(0)))
        .withColumn("revenue", F.col("quantity") * F.col("price"))
        .select(
            "sale_id",
            "sale_date",
            "store_key",
            "product_key",
            "category_key",
            "quantity",
            "price",
            "revenue",
            "has_claim",
        )
    )

    _write_gold(dim_category, config, "dim_category", write_format)
    _write_gold(dim_product, config, "dim_product", write_format)
    _write_gold(dim_store, config, "dim_store", write_format)
    _write_gold(fact_sales, config, "fact_sales", write_format)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev", help="Config environment (dev|prod)")
    args = parser.parse_args()
    main(env=args.env)
