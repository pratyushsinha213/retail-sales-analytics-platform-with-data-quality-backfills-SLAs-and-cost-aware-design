#!/usr/bin/env python3
"""
Check Bronze layer contents: row counts, schema, sample rows, null counts on key columns.
Run from Apple Retail v2: python scripts/check_bronze.py [--env dev]
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
        "check-bronze"
    ).getOrCreate()

    tables = {
        "category": "category_id",
        "products": "Product_ID",
        "sales": "sale_id",
        "stores": "Store_ID",
        "warranty": "claim_id",
    }
    for table_name, pk in tables.items():
        path = get_layer_path(config, "bronze_base", table_name)
        print(f"\n{'='*60}")
        print(f"BRONZE: {table_name}")
        print(f"  path: {path}")
        try:
            df = spark.read.format(read_format).load(path)
            n = df.count()
            print(f"  rows: {n}")
            print(f"  schema: {[f'{f.name}:{f.dataType.simpleString()}' for f in df.schema.fields]}")
            pk_col = pk if pk in df.columns else next((c for c in df.columns if c.lower() == pk.lower()), None)
            if pk_col:
                nulls = df.filter(df[pk_col].isNull()).count()
                print(f"  nulls({pk_col}): {nulls}")
            print("  sample:")
            df.show(3, truncate=40)
        except Exception as e:
            print(f"  ERROR: {e}")
    spark.stop()
    print(f"\n{'='*60} done.\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Check Bronze layer contents")
    p.add_argument("--env", default="dev")
    args = p.parse_args()
    main(env=args.env)
