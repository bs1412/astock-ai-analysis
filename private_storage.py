"""Fail closed on existing private JSON; replace writes only after serialization."""
import copy
import json
import os
from pathlib import Path
import tempfile


class DataReadError(ValueError):
    pass


def read(path, default):
    try:
        with open(path, encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, type(default)):
            raise ValueError("invalid root type")
        if isinstance(value, list) and any(not isinstance(item, dict) for item in value):
            raise ValueError("invalid list item")
        return value
    except FileNotFoundError:
        return copy.deepcopy(default)
    except (OSError, ValueError, TypeError) as error:
        raise DataReadError("账户记录无法读取或结构不合法；原文件保留，未自动重置") from error


def write(path, value):
    # Serialization failure cannot truncate the previous file.
    raw = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
    destination = Path(path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=destination.parent,
                                         prefix=".private-", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(raw)
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
