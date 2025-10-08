# fabric_ceph/response/serialization.py

from dataclasses import is_dataclass, asdict
from datetime import date, datetime
from enum import Enum
from decimal import Decimal
from pathlib import Path
from uuid import UUID
import base64

# Optional: import your Model base to detect generated OpenAPI models directly
try:
    from fabric_ceph.openapi_server.models.base_model import Model  # OpenAPI generator base
except Exception:
    Model = tuple()  # harmless fallback

def deep_to_dict(value, *, _seen=None):
    """Recursively convert models and common Python objects to JSON-serializable primitives."""
    if _seen is None:
        _seen = set()

    # None / primitives
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    # Datetime / date / Enum / Decimal-ish / UUID / Path
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (UUID, Path)):
        return str(value)
    if isinstance(value, Decimal):
        # keep exact string to avoid float rounding; change to float(value) if you prefer numbers
        return str(value)

    # bytes -> base64 text (safe)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")

    # Avoid cycles
    obj_id = id(value)
    if obj_id in _seen:
        return None  # or raise / or str(value)
    _seen.add(obj_id)

    # OpenAPI Model or anything with to_dict()
    if (Model and isinstance(value, Model)) or (hasattr(value, "to_dict") and callable(value.to_dict)):
        try:
            raw = value.to_dict()  # your models should already map to primitives-ish
        except TypeError:
            # Some generators require to_dict(self) without args; just re-raise other issues
            raw = value.to_dict()
        return deep_to_dict(raw, _seen=_seen)

    # Dataclasses
    if is_dataclass(value):
        return deep_to_dict(asdict(value), _seen=_seen)

    # list/tuple/set -> list
    if isinstance(value, (list, tuple, set)):
        return [deep_to_dict(v, _seen=_seen) for v in value]

    # dict -> dict with string keys
    if isinstance(value, dict):
        return {str(k): deep_to_dict(v, _seen=_seen) for k, v in value.items()}

    # Fallback: try __dict__, else string
    if hasattr(value, "__dict__"):
        return {str(k): deep_to_dict(v, _seen=_seen)
                for k, v in value.__dict__.items()
                if not k.startswith("_")}
    return str(value)
