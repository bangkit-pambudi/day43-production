import os
import yaml
from typing import Any


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    return _expand(raw)


def _expand(obj: Any) -> Any:
    """Recursively expand ${ENV_VAR} placeholders with environment variable values."""
    if isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
        key = obj[2:-1]
        return os.environ.get(key, obj)
    if isinstance(obj, dict):
        return {k: _expand(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand(i) for i in obj]
    return obj
