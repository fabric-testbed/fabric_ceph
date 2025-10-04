# fabric_ceph/utils/keyring_parser.py
import json

def keyring_minimal(s: str) -> str:
    """
    Accepts either a JSON-escaped string (with surrounding quotes) or plain text.
    Returns the first two lines + trailing newline.
    """
    # If it's a JSON-escaped string (surrounded by quotes), unescape with json.loads
    if s and (s[0] == '"' and s[-1] == '"'):
        try:
            s = json.loads(s)
        except Exception:
            pass
    # Now s should be real multiline text
    lines = s.splitlines()
    return "\n".join(lines[:2]) + "\n"