#!/usr/bin/env python3
# MIT License
#
# Copyright (component) 2025 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)

from typing import Dict, List, Optional, Tuple

from fabric_ceph.common.config import Config
from fabric_ceph.utils.dash_client import DashClient


def list_users_first_success(
    cfg: Config,
) -> Dict[str, object]:
    """
    Iterate clusters and return the first successful user list.

    Returns:
        {
          "cluster": "<name>",
          "users": [ {...}, {...}, ... ]   # raw Dashboard objects
        }

    Raises:
        RuntimeError if all clusters fail (with per-cluster error details).
    """
    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    errors: Dict[str, str] = {}
    for name, dc in clients.items():
        try:
            users = dc.list_users()
            return {"cluster": name, "users": users}
        except Exception as e:
            errors[name] = str(e)
            continue
    raise RuntimeError(f"list_users failed on all clusters: {errors}")


def export_users_first_success(
    cfg: Config,
    entities: List[str],
) -> Dict[str, object]:
    """
    Try to export the keyring(s) for 'entities' from each cluster in order,
    returning on the first success.

    If DashClient has only export_keyring(entity), exports each entity
    individually and concatenates into a single keyring text blob.

    Returns:
        {
          "cluster": "<name>",
          "keyring": "<combined keyring text>"
        }

    Raises:
        ValueError if entities is empty.
        RuntimeError if all clusters fail (with per-cluster error details).
    """
    if not entities:
        raise ValueError("entities must be a non-empty list")

    errors: Dict[str, str] = {}

    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    for name, dc in clients.items():
        try:
            # Prefer a multi-entity export if your DashClient supports it
            keyring_text: Optional[str] = None
            export_many = getattr(dc, "export_users", None)
            if callable(export_many):
                keyring_text = export_many(entities)  # type: ignore[attr-defined]
            else:
                # Fallback: export each entity and concatenate
                parts: List[str] = []
                for ent in entities:
                    parts.append(dc.export_keyring(ent))
                # Normalize to a single blob; ensure trailing newline for keyring parsers
                keyring_text = ("\n".join(p.strip() for p in parts)).strip() + "\n"

            return {"cluster": name, "keyring": keyring_text}

        except Exception as e:
            errors[name] = str(e)
            continue

    raise RuntimeError(f"export_users failed on all clusters: {errors}")
