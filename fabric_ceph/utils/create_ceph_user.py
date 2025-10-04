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
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from fabric_ceph.common.config import Config, ClusterEntry
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.ssh_runner import SSHCreds, SSHRunner


# ---------- small utility ----------

def _resolve_subvol_path(dc: DashClient, fs_name: str, subvol_name: str, group_name: Optional[str]) -> str:
    """Ask the Dashboard for the subvolume path on THIS cluster."""
    info = dc.get_subvolume_info(fs_name, subvol_name, group_name)
    # Prefer 'path', but accept other common keys
    for k in ("path", "full_path", "mount_path", "mountpoint"):
        p = info.get(k)
        if isinstance(p, str) and p.startswith("/"):
            return p
    # Fallback: any absolute-looking string in the response
    for v in info.values():
        if isinstance(v, str) and v.startswith("/"):
            return v
    raise RuntimeError(f"Could not resolve subvolume path for {fs_name}:{group_name}:{subvol_name} on {dc.cluster_name}")


def _render_caps(base_caps: List[Dict[str, str]], subs: Dict[str, str]) -> List[Dict[str, str]]:
    """Render placeholders in cap strings using the provided substitutions."""
    rendered: List[Dict[str, str]] = []
    for c in base_caps:
        entity = c["entity"]
        cap_tpl = c["cap"]
        # Simple .format with default passthrough for missing keys
        cap = cap_tpl.format(**subs)
        rendered.append({"entity": entity, "cap": cap})
    return rendered


@dataclass
class UserCapsSyncResult:
    user_entity: str
    fs_name: str
    subvol_name: str
    group_name: Optional[str]
    source_cluster: str
    created_on_source: bool
    updated_on_source: bool
    imported_to: List[str]
    caps_applied: Dict[str, List[Dict[str, str]]]  # per-cluster rendered caps
    errors: Dict[str, str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "user_entity": self.user_entity,
            "fs_name": self.fs_name,
            "subvol_name": self.subvol_name,
            "group_name": self.group_name,
            "source_cluster": self.source_cluster,
            "created_on_source": self.created_on_source,
            "updated_on_source": self.updated_on_source,
            "imported_to": self.imported_to,
            "caps_applied": self.caps_applied,
            "errors": self.errors,
        }


# ---------- main orchestration ----------

def ensure_user_across_clusters_with_cluster_paths(
    cfg: Config,
    user_entity: str,
    base_capabilities: List[Dict[str, str]],
    *,
    fs_name: str,
    subvol_name: str,
    group_name: Optional[str] = None,
    preferred_source: Optional[str] = None,
) -> Dict[str, object]:
    """
    Ensure user exists everywhere with the SAME SECRET, but caps rendered with the
    CORRECT PATHS for each cluster.

    base_capabilities should use placeholders, e.g.:
      [
        {"entity":"mon","cap":"allow r"},
        {"entity":"mds","cap":"allow rw fsname={fs} path={path}"},
        {"entity":"osd","cap":"allow rw tag cephfs data={fs}"},
        {"entity":"osd","cap":"allow rw tag cephfs metadata={fs}"}
      ]
    """
    log = logging.getLogger(cfg.logging.logger)
    # build clients (stable order)
    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    # pick source where the user already exists if possible
    source_name: Optional[str] = None
    for name, dc in clients.items():
        try:
            users = dc.list_users()
            if any((u.get("user_entity") or u.get("entity") or u.get("id")) == user_entity for u in users):
                source_name = name
                break
        except Exception:
            log.exception(f"Failed to resolve {user_entity} for cluster {name}")
            continue
    if source_name is None:
        source_name = preferred_source if (preferred_source in clients if preferred_source else False) else next(iter(clients.keys()))

    created_on_source = False
    updated_on_source = False
    caps_applied: Dict[str, List[Dict[str, str]]] = {}
    errors: Dict[str, str] = {}

    # 1) Render caps FOR SOURCE using that cluster's path
    dc_source = clients[source_name]
    try:
        path_src = _resolve_subvol_path(dc_source, fs_name, subvol_name, group_name)
        caps_src = _render_caps(base_capabilities, {"fs": fs_name, "group": group_name or "", "subvol": subvol_name, "path": path_src})
        # update-or-create on source with source-specific caps
        status = dc_source.update_user_caps(user_entity, caps_src)
        if status in (200, 201, 202):
            updated_on_source = True
        else:
            dc_source.create_user(user_entity, caps_src)
            created_on_source = True
            updated_on_source = True
        caps_applied[source_name] = caps_src
    except Exception as e:
        log.exception(f"source update/create failed: {e}")
        errors[source_name] = f"source update/create failed: {e}"
        # If we cannot even set up the source, abort early
        return UserCapsSyncResult(
            user_entity=user_entity, fs_name=fs_name, subvol_name=subvol_name, group_name=group_name,
            source_cluster=source_name, created_on_source=created_on_source, updated_on_source=updated_on_source,
            imported_to=[], caps_applied=caps_applied, errors=errors
        ).to_dict()

    # 2) Export keyring from source (to propagate the same secret)
    try:
        keyring = dc_source.export_keyring(user_entity)
        keyring_bytes = keyring.encode("utf-8")
    except Exception as e:
        log.exception(f"export keyring failed: {e}")
        errors[source_name] = f"export failed: {e}"
        keyring_bytes = None  # we'll still try to proceed per-cluster with updates, but secrets may diverge

    imported_to: List[str] = []

    # 3) For every other cluster: import key (if we have it), then overwrite caps with THIS cluster's path
    for name, dc in clients.items():
        if name == source_name:
            continue
        try:
            # Import the key to keep the SAME SECRET
            if keyring_bytes is not None:
                entry: ClusterEntry = dc.cluster
                ssh = SSHCreds.for_cluster(name, entry)
                remote_tmp = f"/tmp/{user_entity.replace('.', '_')}.keyring.{os.getpid()}"
                with SSHRunner(ssh) as r:
                    r.put_bytes(keyring_bytes, remote_tmp)
                    ceph_cli = entry.ceph_cli or "ceph"
                    r.run(f"{ceph_cli} auth import -i {remote_tmp}")
                    r.run(f"rm -f {remote_tmp}", check=False)
                imported_to.append(name)

            # Render caps for THIS cluster
            path_here = _resolve_subvol_path(dc, fs_name, subvol_name, group_name)
            caps_here = _render_caps(base_capabilities, {"fs": fs_name, "group": group_name or "", "subvol": subvol_name, "path": path_here})
            caps_applied[name] = caps_here

            # Overwrite caps via REST
            _ = dc.update_user_caps(user_entity, caps_here)

        except Exception as e:
            log.exception(f"import user and update_user_caps failed: {e}")
            errors[name] = str(e)
            continue

    return UserCapsSyncResult(
        user_entity=user_entity,
        fs_name=fs_name,
        subvol_name=subvol_name,
        group_name=group_name,
        source_cluster=source_name,
        created_on_source=created_on_source,
        updated_on_source=updated_on_source,
        imported_to=imported_to,
        caps_applied=caps_applied,
        errors=errors,
    ).to_dict()
