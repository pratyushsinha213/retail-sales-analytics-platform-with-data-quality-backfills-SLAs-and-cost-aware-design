"""
Local pipeline entrypoint: ingest -> bronze_to_silver -> silver_to_gold -> data_quality.
Run from project root: python main.py [--env dev] [--skip-dq]
"""
import argparse

from ingestion.ingest_raw import main as ingest_main
from transformations.bronze_to_silver import main as silver_main
from transformations.silver_to_gold import main as gold_main
from data_quality.run_checks import main as dq_main


def main():
    parser = argparse.ArgumentParser(description="Run full pipeline locally (ingest → silver → gold → DQ)")
    parser.add_argument("--env", default="dev", help="Config environment (dev|prod)")
    parser.add_argument("--skip-dq", action="store_true", help="Skip data quality checks at the end")
    parser.add_argument("--fail-dq", action="store_true", help="Exit with code 1 if DQ checks fail")
    args = parser.parse_args()

    ingest_main(env=args.env)
    silver_main(env=args.env)
    gold_main(env=args.env)
    if not args.skip_dq:
        dq_main(env=args.env, layer="all", fail_on_error=args.fail_dq)


if __name__ == "__main__":
    main()
