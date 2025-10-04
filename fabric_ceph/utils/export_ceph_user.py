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
import logging
from typing import Dict, List, Optional, Tuple

from fabric_ceph.common.config import Config
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.keyring_parser import keyring_minimal


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
    logger = logging.getLogger(cfg.logging.logger)
    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    errors: Dict[str, str] = {}
    for name, dc in clients.items():
        try:
            users = dc.list_users()
            return {"cluster": name, "users": users}
        except Exception as e:
            logger.exception(f"Encountered exception while fetching users on {name}")
            errors[name] = str(e)
            continue
    raise RuntimeError(f"list_users failed on all clusters: {errors}")


def export_users_first_success(
    cfg: Config,
    entities: List[str],
    keyring_only: bool,
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
    logger = logging.getLogger(cfg.logging.logger)
    if not entities:
        raise ValueError("entities must be a non-empty list")

    errors: Dict[str, str] = {}

    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    result = {}

    for name, dc in clients.items():
        try:
            exported_entities = {}
            for ent in entities:
                try:
                    keyring = dc.export_keyring(ent)
                    exported_entities[ent] = keyring
                    if keyring_only:
                        logger.debug(f"Exported keyring {keyring}")
                        key = keyring_minimal(keyring)
                        if not key:
                            raise RuntimeError("key not found in exported keyring")
                        exported_entities[ent] = key
                except Exception as e:
                    errors[name] = str(e)

            result[name] = exported_entities
        except Exception as e:
            logger.exception(f"Encountered exception while exporting users on {name}")
            errors[name] = str(e)
            continue

    if len(errors) > 0 and len(result) == 0:
        details = " ".join(f"{k}:{v}" for k, v in errors.items())
        raise ValueError("No users were exported: {}".format(details))

    return result
