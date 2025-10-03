# fabric_ceph/utils/keyring_parser.py
import re
from typing import Optional

_KEY_RE = re.compile(r'^\s*key\s*=\s*([A-Za-z0-9+/=]+)\s*$', re.M)

def extract_key_from_keyring(keyring_text: str, entity: str) -> Optional[str]:
    """
    Extract the base64 secret for the given entity from a Ceph keyring block.

    Robust for multi-entity exports:
      [client.foo]
          key = <...>
      [client.bar]
          key = <...>

    Returns base64 string or None if not found.
    """
    if not keyring_text or not entity:
        return None

    # Find the right section:
    # Split on lines that look like [client.something]
    sections = re.split(r'(?m)^\s*\[([^\]]+)\]\s*$', keyring_text)
    # re.split returns: [pre, sect1_name, sect1_body, sect2_name, sect2_body, ...]
    if len(sections) < 3:
        # Single section or just raw; try a simple key= search
        m = _KEY_RE.search(keyring_text)
        return m.group(1) if m else None

    it = iter(sections[1:])  # skip the prelude
    for sect_name, sect_body in zip(it, it):
        if sect_name.strip() == entity:
            m = _KEY_RE.search(sect_body)
            return m.group(1) if m else None

    return None
