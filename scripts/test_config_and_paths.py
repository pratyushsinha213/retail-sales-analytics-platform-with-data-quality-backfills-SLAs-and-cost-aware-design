#!/usr/bin/env python3
"""
Test config loading and path resolution without Spark.
Run from Apple Retail v2: python scripts/test_config_and_paths.py [--env dev|prod]
"""
import argparse
import os
import sys

# Run from project root so imports work
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from utils.config_loader import get_config
from utils.path_utils import get_layer_path, get_raw_csv_path, get_project_root


def main(env="dev"):
    print(f"Project root: {get_project_root()}")
    print(f"Loading config env={env!r}...")
    config = get_config(env)
    print(f"  use_local_paths: {config.get('use_local_paths')}")
    print(f"  lake_base_path: {config.get('lake_base_path')}")
    print(f"  s3_bucket: {config.get('s3_bucket')}")
    print(f"  s3_prefix: {config.get('s3_prefix')}")
    print()

    print("Layer paths (bronze/silver/gold):")
    for layer_key in ("bronze_base", "silver_base", "gold_base"):
        for table in ["category", "sales"]:
            path = get_layer_path(config, layer_key, table)
            print(f"  {layer_key} / {table}: {path}")
    print()

    print("Raw CSV paths:")
    for name in ["category.csv", "sales.csv"]:
        path = get_raw_csv_path(config, name)
        print(f"  {name}: {path}")
        if config.get("use_local_paths") and path.startswith("/"):
            exists = "yes" if os.path.exists(path) else "MISSING"
            print(f"    exists: {exists}")
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test config and path utils")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    args = parser.parse_args()
    main(env=args.env)
