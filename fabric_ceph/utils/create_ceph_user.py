#!/usr/bin/env python3
# MIT License
#
# Author: Komal Thareja (kthare10@renci.org)
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

from fabric_ceph.common.config import Config, ClusterEntry
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.ssh_runner import SSHCreds, SSHRunner


# ---------- small utilities ----------

def _normalize_info_obj(info: Any) -> Dict[str, Any]:
    """Some client/server stacks return (obj, status[, headers]). Peel to a dict."""
    if isinstance(info, (list, tuple)) and info:
        info = info[0]
    if not isinstance(info, dict):
        return {}
    return info


def _resolve_subvol_path(
    dc: DashClient, cluster_name: str, fs_name: str, subvol_name: str, group_name: Optional[str]
) -> str:
    """Ask the Dashboard for the subvolume path on THIS cluster."""
    info_raw = dc.get_subvolume_info(fs_name, subvol_name, group_name)
    info = _normalize_info_obj(info_raw)

    # Prefer 'path', but accept other common keys
    for k in ("path", "full_path", "mount_path", "mountpoint"):
        p = info.get(k)
        if isinstance(p, str) and p.startswith("/"):
            return p

    # Fallback: any absolute-looking string in the dict
    for v in info.values():
        if isinstance(v, str) and v.startswith("/"):
            return v

    raise RuntimeError(
        f"Could not resolve subvolume path for fs={fs_name} group={group_name} subvol={subvol_name} on cluster={cluster_name}"
    )


class _SafeMap(dict):
    """format_map helper that leaves unknown placeholders intact, e.g. '{foo}'."""
    def __missing__(self, key: str) -> str:  # type: ignore[override]
        return "{" + key + "}"


def _render_caps(base_caps: List[Dict[str, str]], subs: Dict[str, str]) -> List[Dict[str, str]]:
    """Render placeholders in cap strings using the provided substitutions (safe)."""
    out: List[Dict[str, str]] = []
    sm = _SafeMap(**subs)
    for c in base_caps:
        entity = c.get("entity", "").strip()
        cap_tpl = c.get("cap", "")
        cap = str(cap_tpl).format_map(sm)
        out.append({"entity": entity, "cap": cap})
    return out


# ---------- results ----------

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
    log = logging.getLogger(getattr(cfg.logging, "logger", __name__))

    # Build clients (stable order)
    clients: Dict[str, DashClient] = {
        name: DashClient.for_cluster(name, entry) for name, entry in cfg.cluster.items()
    }

    # Pick source where the user already exists if possible
    source_name: Optional[str] = None
    for name, dc in clients.items():
        try:
            users = dc.list_users()
            if any((u.get("user_entity") or u.get("entity") or u.get("id")) == user_entity for u in users):
                source_name = name
                break
        except Exception as e:
            log.warning("Failed to list users on cluster %s: %s", name, e)
            continue

    if not source_name:
        if preferred_source and preferred_source in clients:
            source_name = preferred_source
        else:
            source_name = next(iter(clients))  # first configured cluster

    created_on_source = False
    updated_on_source = False
    caps_applied: Dict[str, List[Dict[str, str]]] = {}
    errors: Dict[str, str] = {}

    # 1) Render caps FOR SOURCE using that cluster's path
    dc_source = clients[source_name]
    try:
        path_src = _resolve_subvol_path(dc_source, source_name, fs_name, subvol_name, group_name)
        caps_src = _render_caps(
            base_capabilities,
            {"fs": fs_name, "group": group_name or "", "subvol": subvol_name, "path": path_src},
        )

        # Update-or-create on source with source-specific caps
        status = dc_source.update_user_caps(user_entity, caps_src)
        if isinstance(status, int) and status in (200, 201, 202):
            updated_on_source = True
        else:
            # Some clients return JSON instead of status; treat as success
            updated_on_source = True

    except Exception:
        # Try create on source if update failed
        try:
            dc_source.create_user(user_entity, caps_src)  # type: ignore[name-defined]
            created_on_source = True
            updated_on_source = True
        except Exception as e2:
            msg = f"source update/create failed: {e2}"
            log.exception(msg)
            errors[source_name] = msg
            return UserCapsSyncResult(
                user_entity=user_entity,
                fs_name=fs_name,
                subvol_name=subvol_name,
                group_name=group_name,
                source_cluster=source_name,
                created_on_source=created_on_source,
                updated_on_source=updated_on_source,
                imported_to=[],
                caps_applied=caps_applied,
                errors=errors,
            ).to_dict()

    caps_applied[source_name] = caps_src  # type: ignore[name-defined]

    # 2) Export keyring from source (to propagate the same secret)
    keyring_bytes: Optional[bytes]
    try:
        keyring = dc_source.export_keyring(user_entity)
        keyring_bytes = keyring.encode("utf-8")
    except Exception as e:
        log.warning("Export keyring failed on %s: %s (will proceed without import)", source_name, e)
        errors[source_name] = f"export failed: {e}"
        keyring_bytes = None  # proceed; secrets may diverge

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
            path_here = _resolve_subvol_path(dc, name, fs_name, subvol_name, group_name)
            caps_here = _render_caps(
                base_capabilities,
                {"fs": fs_name, "group": group_name or "", "subvol": subvol_name, "path": path_here},
            )
            caps_applied[name] = caps_here

            # Overwrite caps via REST (treat JSON return as success)
            _ = dc.update_user_caps(user_entity, caps_here)

        except Exception as e:
            msg = f"import/update caps failed: {e}"
            log.exception("Cluster %s: %s", name, msg)
            errors[name] = msg
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
