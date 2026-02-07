"""
AWS Glue entrypoint: runs ingest -> bronze_to_silver -> silver_to_gold -> data_quality.run_checks.
Set job parameters in Terraform: --s3_bucket, --s3_prefix, --region.
Upload this script and app.zip (config, utils, ingestion, transformations, data_quality) to S3;
Glue adds app.zip to PYTHONPATH so imports work.
"""
import os
import sys

# Set config from Glue job arguments so get_config("prod") uses env (no YAML in S3)
try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_bucket", "s3_prefix", "region"])
    os.environ["AWS_S3_BUCKET"] = args["s3_bucket"]
    os.environ["AWS_S3_PREFIX"] = args.get("s3_prefix", "")
    os.environ["AWS_REGION"] = args.get("region", "us-east-1")
    os.environ["USE_LOCAL_PATHS"] = "false"
except Exception:
    pass

# Ensure project packages are on path (app.zip from --extra-py-files is usually already there)
_script_dir = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_script_dir)
if _parent not in sys.path:
    sys.path.insert(0, _parent)


def main():
    env = "prod"
    from ingestion.ingest_raw import main as ingest_main
    from transformations.bronze_to_silver import main as silver_main
    from transformations.silver_to_gold import main as gold_main
    from data_quality.run_checks import main as dq_main

    ingest_main(env=env)
    silver_main(env=env)
    gold_main(env=env)
    dq_main(env=env, layer="all", fail_on_error=False)


if __name__ == "__main__":
    main()
