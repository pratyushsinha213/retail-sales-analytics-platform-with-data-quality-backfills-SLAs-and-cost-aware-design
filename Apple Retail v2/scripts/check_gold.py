#!/usr/bin/env python3
"""
Check Gold layer contents: row counts, schema, sample rows, nulls on keys, referential check.
Run from Apple Retail v2: python scripts/check_gold.py [--env dev]
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from utils.config_loader import get_config
from utils.path_utils import get_layer_path, get_project_root
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(env="dev"):
    config = get_config(env)
    if config.get("use_local_paths"):
        os.chdir(get_project_root())

    use_local = config.get("use_local_paths", False)
    read_format = "parquet" if use_local else "delta"

    spark = SparkSession.builder.appName("check-gold").getOrCreate()

    dims = ["dim_category", "dim_product", "dim_store"]
    for table_name in dims:
        path = get_layer_path(config, "gold_base", table_name)
        print(f"\n{'='*60}")
        print(f"GOLD: {table_name}")
        print(f"  path: {path}")
        try:
            df = spark.read.format(read_format).load(path)
            n = df.count()
            print(f"  rows: {n}")
            print(f"  schema: {[f'{f.name}:{f.dataType.simpleString()}' for f in df.schema.fields]}")
            key_col = "category_key" if "category_key" in df.columns else "product_key" if "product_key" in df.columns else "store_key"
            nulls = df.filter(F.col(key_col).isNull()).count()
            print(f"  nulls({key_col}): {nulls}")
            print("  sample:")
            df.show(3, truncate=40)
        except Exception as e:
            print(f"  ERROR: {e}")

    # fact_sales
    path = get_layer_path(config, "gold_base", "fact_sales")
    print(f"\n{'='*60}")
    print("GOLD: fact_sales")
    print(f"  path: {path}")
    try:
        fact = spark.read.format(read_format).load(path)
        n = fact.count()
        print(f"  rows: {n}")
        print(f"  schema: {[f'{f.name}:{f.dataType.simpleString()}' for f in fact.schema.fields]}")
        for col in ["sale_id", "store_key", "product_key", "category_key", "revenue", "has_claim"]:
            if col in fact.columns:
                nulls = fact.filter(F.col(col).isNull()).count()
                print(f"  nulls({col}): {nulls}")
        if "revenue" in fact.columns:
            rev = fact.agg(F.sum("revenue").alias("total")).collect()[0]["total"]
            print(f"  sum(revenue): {rev}")
        print("  sample:")
        fact.show(3, truncate=40)
    except Exception as e:
        print(f"  ERROR: {e}")

    # Referential: fact keys should exist in dims (optional quick check)
    print(f"\n{'='*60}")
    print("Referential check (fact_sales keys in dims)")
    try:
        fact = spark.read.format(read_format).load(get_layer_path(config, "gold_base", "fact_sales"))
        dim_c = spark.read.format(read_format).load(get_layer_path(config, "gold_base", "dim_category"))
        dim_p = spark.read.format(read_format).load(get_layer_path(config, "gold_base", "dim_product"))
        dim_s = spark.read.format(read_format).load(get_layer_path(config, "gold_base", "dim_store"))
        fact_with_c = fact.join(dim_c.select("category_key"), on="category_key", how="left_anti")
        fact_with_p = fact.join(dim_p.select("product_key"), on="product_key", how="left_anti")
        fact_with_s = fact.join(dim_s.select("store_key"), on="store_key", how="left_anti")
        orphan_c = fact_with_c.count()
        orphan_p = fact_with_p.count()
        orphan_s = fact_with_s.count()
        print(f"  fact rows with category_key not in dim_category: {orphan_c}")
        print(f"  fact rows with product_key not in dim_product: {orphan_p}")
        print(f"  fact rows with store_key not in dim_store: {orphan_s}")
    except Exception as e:
        print(f"  ERROR: {e}")

    spark.stop()
    print(f"\n{'='*60} done.\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Check Gold layer contents")
    p.add_argument("--env", default="dev")
    args = p.parse_args()
    main(env=args.env)
