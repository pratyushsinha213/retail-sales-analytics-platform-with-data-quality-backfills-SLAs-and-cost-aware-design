"""
Load pipeline config from YAML (local) or from environment (AWS Glue).
When Glue sets AWS_S3_BUCKET and USE_LOCAL_PATHS=false, config is built from env.
"""
import os
import yaml
from pathlib import Path


def get_project_root():
    return Path(__file__).resolve().parent.parent


def get_config(env="dev"):
    """
    Config dict. If AWS Glue env vars are set, build from them; else load config/<env>.yaml.
    Glue job should set: AWS_S3_BUCKET, AWS_S3_PREFIX (optional), AWS_REGION, USE_LOCAL_PATHS=false.
    """
    if os.environ.get("AWS_S3_BUCKET") and os.environ.get("USE_LOCAL_PATHS", "").lower() == "false":
        return {
            "environment": env,
            "use_local_paths": False,
            "lake_base_path": None,
            "s3_bucket": os.environ["AWS_S3_BUCKET"],
            "s3_prefix": os.environ.get("AWS_S3_PREFIX", "").strip(),
            "region": os.environ.get("AWS_REGION", "us-east-1"),
            "bronze_base": "bronze",
            "silver_base": "silver",
            "gold_base": "gold",
            "raw_data_path": None,
            "max_late_days": 7,
        }
    root = get_project_root()
    path = root / "config" / f"{env}.yaml"
    with open(path) as f:
        return yaml.safe_load(f)
