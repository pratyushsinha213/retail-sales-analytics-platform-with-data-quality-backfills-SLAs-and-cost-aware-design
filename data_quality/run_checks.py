"""
Run data quality checks on Silver and Gold (row counts, nulls on keys, value ranges, referential).
No Great Expectations – plain Spark checks. Use after the pipeline.
Run from Apple Retail v2: python -m data_quality.run_checks [--env dev] [--layer silver|gold|all] [--fail]
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


def _read(spark, config, layer_key, table, fmt):
    path = get_layer_path(config, layer_key, table)
    return spark.read.format(fmt).load(path)


def check_silver(spark, config, fmt):
    failed = []
    # Silver: non-null PKs, sales quantity in range
    tables_pk = [("category", "category_id"), ("products", "product_id"), ("sales", "sale_id"), ("stores", "store_id"), ("warranty", "claim_id")]
    for table, pk in tables_pk:
        df = _read(spark, config, "silver_base", table, fmt)
        n = df.count()
        if n == 0:
            failed.append(f"silver.{table}: row count 0")
            continue
        nulls = df.filter(F.col(pk).isNull()).count()
        if nulls > 0:
            failed.append(f"silver.{table}: {nulls} nulls in {pk}")
    df_sales = _read(spark, config, "silver_base", "sales", fmt)
    bad_qty = df_sales.filter((F.col("quantity").isNull()) | (F.col("quantity") < 1)).count()
    if bad_qty > 0:
        failed.append(f"silver.sales: {bad_qty} rows with quantity null or < 1")
    return failed


def check_gold(spark, config, fmt):
    failed = []
    fact = _read(spark, config, "gold_base", "fact_sales", fmt)
    n = fact.count()
    if n == 0:
        failed.append("gold.fact_sales: row count 0")
    else:
        for col in ["sale_id", "store_key", "product_key", "category_key", "revenue"]:
            nulls = fact.filter(F.col(col).isNull()).count()
            if nulls > 0:
                failed.append(f"gold.fact_sales: {nulls} nulls in {col}")
        bad_rev = fact.filter(F.col("revenue") < 0).count()
        if bad_rev > 0:
            failed.append(f"gold.fact_sales: {bad_rev} rows with revenue < 0")
        bad_claim = fact.filter(~F.col("has_claim").isin([0, 1])).count()
        if bad_claim > 0:
            failed.append(f"gold.fact_sales: {bad_claim} rows with has_claim not in (0,1)")

    dim_c = _read(spark, config, "gold_base", "dim_category", fmt)
    dim_p = _read(spark, config, "gold_base", "dim_product", fmt)
    dim_s = _read(spark, config, "gold_base", "dim_store", fmt)
    orphan_c = fact.join(dim_c.select("category_key"), "category_key", "left_anti").count()
    orphan_p = fact.join(dim_p.select("product_key"), "product_key", "left_anti").count()
    orphan_s = fact.join(dim_s.select("store_key"), "store_key", "left_anti").count()
    if orphan_c > 0:
        failed.append(f"gold.fact_sales: {orphan_c} rows with category_key not in dim_category")
    if orphan_p > 0:
        failed.append(f"gold.fact_sales: {orphan_p} rows with product_key not in dim_product")
    if orphan_s > 0:
        failed.append(f"gold.fact_sales: {orphan_s} rows with store_key not in dim_store")
    return failed


def main(env: str = "dev", layer: str = "all", fail_on_error: bool = False) -> None:
    config = get_config(env)
    if config.get("use_local_paths"):
        os.chdir(get_project_root())
    fmt = "parquet" if config.get("use_local_paths", False) else "delta"

    spark = SparkSession.builder.appName("dq-checks").getOrCreate()
    all_failed = []

    if layer in ("silver", "all"):
        all_failed.extend(check_silver(spark, config, fmt))
    if layer in ("gold", "all"):
        all_failed.extend(check_gold(spark, config, fmt))

    spark.stop()

    if not all_failed:
        print("Data quality: PASS")
        return
    print("Data quality: FAIL")
    for msg in all_failed:
        print(f"  - {msg}")
    if fail_on_error:
        sys.exit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Run data quality checks (Silver/Gold)")
    p.add_argument("--env", default="dev")
    p.add_argument("--layer", default="all", choices=["silver", "gold", "all"])
    p.add_argument("--fail", action="store_true", help="Exit with code 1 if any check fails")
    args = p.parse_args()
    main(env=args.env, layer=args.layer, fail_on_error=args.fail)
