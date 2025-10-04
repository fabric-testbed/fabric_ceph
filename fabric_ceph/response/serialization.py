# fabric_ceph/openapi_server/serialization.py
from datetime import date, datetime
from enum import Enum

def deep_to_dict(value):
    """Recursively convert model objects, lists, and dicts to plain Python types."""
    if value is None:
        return None

    # Model object with to_dict()
    if hasattr(value, "to_dict") and callable(value.to_dict):
        # Avoid infinite recursion if model.to_dict calls deep_to_dict again.
        # We call its own to_dict, but ensure nested fields are also normalized.
        raw = value.to_dict()
        return deep_to_dict(raw)

    # Datetime / date / Enum
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value

    # List / tuple
    if isinstance(value, (list, tuple)):
        return [deep_to_dict(v) for v in value]

    # Dict (this is the key part for Dict[str, List[Model]])
    if isinstance(value, dict):
        return {k: deep_to_dict(v) for k, v in value.items()}

    # Primitives (str/int/float/bool/None)
    return value
