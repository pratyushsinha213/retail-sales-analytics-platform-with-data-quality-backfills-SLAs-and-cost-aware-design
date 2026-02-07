#!/usr/bin/env python3
"""
Check Silver layer contents: row counts, schema, sample rows, null counts on key columns.
Run from Apple Retail v2: python scripts/check_silver.py [--env dev]
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from utils.config_loader import get_config
from utils.path_utils import get_layer_path, get_project_root


def main(env="dev"):
    config = get_config(env)
    if config.get("use_local_paths"):
        os.chdir(get_project_root())

    use_local = config.get("use_local_paths", False)
    read_format = "parquet" if use_local else "delta"

    spark = __import__("pyspark.sql", fromlist=["SparkSession"]).SparkSession.builder.appName(
        "check-silver"
    ).getOrCreate()

    tables = {
        "category": "category_id",
        "products": "product_id",
        "sales": "sale_id",
        "stores": "store_id",
        "warranty": "claim_id",
    }
    for table_name, pk in tables.items():
        path = get_layer_path(config, "silver_base", table_name)
        print(f"\n{'='*60}")
        print(f"SILVER: {table_name}")
        print(f"  path: {path}")
        try:
            df = spark.read.format(read_format).load(path)
            n = df.count()
            print(f"  rows: {n}")
            print(f"  schema: {[f'{f.name}:{f.dataType.simpleString()}' for f in df.schema.fields]}")
            if pk in df.columns:
                nulls = df.filter(df[pk].isNull()).count()
                print(f"  nulls({pk}): {nulls}")
            if table_name == "sales" and "quantity" in df.columns:
                min_q = df.agg({"quantity": "min"}).collect()[0][0]
                print(f"  quantity min: {min_q}")
            print("  sample:")
            df.show(3, truncate=40)
        except Exception as e:
            print(f"  ERROR: {e}")
    spark.stop()
    print(f"\n{'='*60} done.\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Check Silver layer contents")
    p.add_argument("--env", default="dev")
    args = p.parse_args()
    main(env=args.env)
