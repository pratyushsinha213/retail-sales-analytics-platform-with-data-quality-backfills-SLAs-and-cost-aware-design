"""
Path helpers for Bronze/Silver/Gold layers.
Local: writes under lake_base_path (e.g. ./data_lake).
AWS: builds s3://bucket/prefix/<layer>/<table>.
"""
import os
from typing import Dict, Any, Optional, Literal


def get_project_root() -> str:
    """Project root = parent of 'utils' (Apple Retail v2 folder)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


LayerBaseKey = Literal['bronze_base', 'silver_base', 'gold_base']


def get_layer_path(
    config: Dict[str, Any],
    layer_base_key: LayerBaseKey,
    table_name: str,
) -> str:
    """
    Full path for a table in a layer.
    layer_base_key: 'bronze_base' | 'silver_base' | 'gold_base'
    table_name: e.g. 'sales', 'category', 'products', 'stores', 'warranty'
    """
    use_local: bool = bool(config.get("use_local_paths", False))
    base: str = str(config.get("lake_base_path") or ".")

    if use_local:
        path: str = os.path.join(base, config[layer_base_key], table_name)
        if not path.startswith("file://") and not os.path.isabs(path):
            path = os.path.abspath(path)
        return path
    else:
        bucket: str = config["s3_bucket"]
        prefix: str = (config.get("s3_prefix") or "").strip()
        layer_folder: str = config[layer_base_key]
        if prefix:
            return f"s3://{bucket}/{prefix.strip('/')}/{layer_folder}/{table_name}"
        return f"s3://{bucket}/{layer_folder}/{table_name}"


def get_raw_csv_path(
    config: Dict[str, Any],
    filename: str,
    project_root: Optional[str] = None,
) -> str:
    """
    Path to a raw CSV (e.g. category.csv).
    Local: raw_data_path + filename (relative to project_root).
    AWS (when not local and no raw_data_path): s3://bucket/prefix/raw/filename
    """
    use_local: bool = bool(config.get("use_local_paths"))
    raw_data_path: Optional[str] = config.get("raw_data_path")
    s3_bucket: Optional[str] = config.get("s3_bucket")

    if not use_local and not raw_data_path and s3_bucket:
        prefix: str = (config.get("s3_prefix") or "").strip()
        base: str = f"s3://{s3_bucket}"
        if prefix:
            base = f"{base}/{prefix.strip('/')}"
        return f"{base}/raw/{filename}"

    raw: str = raw_data_path or "."
    path: str = os.path.join(raw, filename)
    if not os.path.isabs(path) and not path.startswith("file://") and not path.startswith("s3://"):
        root: str = project_root or get_project_root()
        path = os.path.abspath(os.path.join(root, path))
    return path
