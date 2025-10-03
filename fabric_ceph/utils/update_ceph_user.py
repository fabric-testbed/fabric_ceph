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
from dataclasses import dataclass
from typing import List, Dict, Optional
import os

from fabric_ceph.common.config import Config, ClusterEntry
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.ssh_runner import SSHCreds, SSHRunner

@dataclass
class UpdateSyncResult:
    source_cluster: str
    updated_on_source: bool
    created_on_source: bool
    imported_to: List[str]
    key_ring: str

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_cluster": self.source_cluster,
            "updated_on_source": self.updated_on_source,
            "created_on_source": self.created_on_source,
            "imported_to": self.imported_to,
            "key_ring": self.key_ring,
        }

def update_user_across_clusters(
    cfg: Config,
    user_entity: str,
    capabilities: List[Dict[str, str]],
    preferred_source: Optional[str] = None,
) -> Dict[str, object]:
    """
    Ensure `user_entity` has the provided capabilities everywhere **and**
    the same secret across clusters.

    Algo:
      - If the user exists on any cluster, choose that as SOURCE.
      - PUT (update caps) on SOURCE; if missing there, create it.
      - Export SOURCE keyring.
      - SSH-import that keyring on every other cluster (creates/updates there).
    """
    logger = logging.getLogger(cfg.logging.logger)
    # Build clients
    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    # Find an existing source
    source_name: Optional[str] = None
    for name, dc in clients.items():
        try:
            users = dc.list_users()
            if any((u.get("user_entity") or u.get("entity") or u.get("id")) == user_entity for u in users):
                source_name = name
                break
        except Exception:
            logger.exception(f"Encountered exception while getting {user_entity} on {name}")
            continue

    created_on_source = False
    updated_on_source = False

    # Choose a source if none found
    if source_name is None:
        if preferred_source and preferred_source in clients:
            source_name = preferred_source
        else:
            source_name = next(iter(clients.keys()))

    # Update caps on source (create if needed)
    dc_source = clients[source_name]
    status = dc_source.update_user_caps(user_entity, capabilities)
    if status in (200, 201, 202):
        updated_on_source = True
    else:
        # Missing? create then it's considered created_on_source
        dc_source.create_user(user_entity, capabilities)
        created_on_source = True
        updated_on_source = True  # it now has desired caps by creation

    # Export keyring from source AFTER caps are correct (so keyring has new caps)
    keyring = dc_source.export_keyring(user_entity)
    keyring_bytes = keyring.encode("utf-8")

    # Import to the rest via SSH
    imported_to: List[str] = []
    for name, dc in clients.items():
        if name == source_name:
            continue
        entry: ClusterEntry = dc.cluster
        ssh = SSHCreds.for_cluster(name, entry)
        remote_tmp = f"/tmp/{user_entity.replace('.', '_')}.keyring.{os.getpid()}"
        with SSHRunner(ssh) as r:
            r.put_bytes(keyring_bytes, remote_tmp)
            ceph_cli = entry.ceph_cli or "ceph"
            r.run(f"{ceph_cli} auth import -i {remote_tmp}")
            r.run(f"rm -f {remote_tmp}", check=False)
        imported_to.append(name)

    return UpdateSyncResult(
        source_cluster=source_name,
        updated_on_source=updated_on_source,
        created_on_source=created_on_source,
        imported_to=imported_to,
        key_ring=keyring,
    ).to_dict()
