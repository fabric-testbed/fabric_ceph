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
from dataclasses import dataclass
from typing import Dict, List, Optional

from fabric_ceph.common.config import Config
from fabric_ceph.utils.dash_client import DashClient

# ---------- results ----------

@dataclass
class SubvolSyncResult:
    fs_name: str
    group_name: Optional[str]
    subvol_name: str
    requested_size: Optional[int]
    requested_mode: Optional[str]
    source_cluster: str
    existed_on_source: bool
    created_on_source: bool
    applied: Dict[str, str]            # cluster -> "created"|"resized"|"ok"
    paths: Dict[str, str]              # cluster -> path
    errors: Dict[str, str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "fs_name": self.fs_name,
            "group_name": self.group_name,
            "subvol_name": self.subvol_name,
            "requested_size": self.requested_size,
            "requested_mode": self.requested_mode,
            "source_cluster": self.source_cluster,
            "existed_on_source": self.existed_on_source,
            "created_on_source": self.created_on_source,
            "applied": self.applied,
            "paths": self.paths,
            "errors": self.errors,
        }

@dataclass
class SubvolDeleteResult:
    fs_name: str
    group_name: Optional[str]
    subvol_name: str
    deleted_from: List[str]
    not_found: List[str]
    errors: Dict[str, str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "fs_name": self.fs_name,
            "group_name": self.group_name,
            "subvol_name": self.subvol_name,
            "deleted_from": self.deleted_from,
            "not_found": self.not_found,
            "errors": self.errors,
        }

# ---------- provisioning ----------

def ensure_subvolume_across_clusters(
    cfg: Config,
    fs_name: str,
    subvol_name: str,
    group_name: Optional[str] = None,
    size_bytes: Optional[int] = None,
    mode: Optional[str] = None,
    preferred_source: Optional[str] = None,
) -> Dict[str, object]:
    """
    Ensure a CephFS subvolume (and its group) exists across all clusters with the same name,
    and apply 'size_bytes' (quota) consistently.

    Algo:
      - Build clients for all clusters.
      - Choose SOURCE: first cluster where subvolume already exists; else preferred/first.
      - On SOURCE: ensure group; create (with mode) if missing, else resize if size specified.
      - On all other clusters: ensure group; create/resize with same parameters (idempotent).
      - Gather 'path' for each cluster via info() and return a consolidated result.
    """
    logger = logging.getLogger(cfg.logging.logger)
    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    # 1) find source cluster with existing subvol
    source_name: Optional[str] = None
    for name, dc in clients.items():
        try:
            if dc.subvolume_exists(fs_name, subvol_name, group_name):
                source_name = name
                break
        except Exception:
            logger.warning("Subvolume %s with group %s does not exist on %s",
                           subvol_name, group_name, fs_name)
            continue

    existed_on_source = source_name is not None
    created_on_source = False

    if source_name is None:
        if preferred_source and preferred_source in clients:
            source_name = preferred_source
        else:
            source_name = next(iter(clients.keys()))

    applied: Dict[str, str] = {}
    paths: Dict[str, str] = {}
    errors: Dict[str, str] = {}

    # Helper to apply on one cluster
    def _apply(dc: DashClient, name: str, is_source: bool) -> None:
        nonlocal created_on_source
        try:
            # group
            if group_name:
                dc.ensure_subvol_group(fs_name, group_name)

            exists = dc.subvolume_exists(fs_name, subvol_name, group_name)
            if exists:
                # resize if quota specified; otherwise no-op "ok"
                if size_bytes is not None and int(size_bytes) >= 0:
                    dc.resize_subvolume(fs_name, subvol_name, group_name, size_bytes=size_bytes)
                    applied[name] = "resized"
                else:
                    applied[name] = "ok"
            else:
                # create; pass mode if provided; omit size to create unlimited
                dc.create_subvolume(
                    fs_name, subvol_name, group_name, size_bytes=size_bytes, mode=mode
                )
                applied[name] = "created"
                if is_source:
                    created_on_source = True

            # fetch info to capture path
            info = dc.get_subvolume_info(fs_name, subvol_name, group_name)
            spath = None
            for k in ("path", "full_path", "mount_path", "mountpoint"):
                if isinstance(info, dict) and isinstance(info.get(k), str) and info[k].startswith("/"):
                    spath = info[k]
                    break
            if not spath:
                # last resort: accept any absolute path-looking string
                spath = next((v for v in info.values() if isinstance(v, str) and v.startswith("/")), "")
            if spath:
                paths[name] = spath

        except Exception as e:
            logger.error("Subvolume %s with group %s could not be created on %s at cluster %s",
                        subvol_name, group_name, fs_name, name)
            logger.exception(e)

            errors[name] = str(e)

    # 2) source first
    dc_source = clients[source_name]
    _apply(dc_source, source_name, is_source=True)

    # 3) the rest
    for name, dc in clients.items():
        if name == source_name:
            continue
        _apply(dc, name, is_source=False)

    return SubvolSyncResult(
        fs_name=fs_name,
        group_name=group_name,
        subvol_name=subvol_name,
        requested_size=(int(size_bytes) if size_bytes is not None else None),
        requested_mode=(str(mode) if mode else None),
        source_cluster=source_name,
        existed_on_source=existed_on_source,
        created_on_source=created_on_source,
        applied=applied,
        paths=paths,
        errors=errors,
    ).to_dict()


def delete_subvolume_across_clusters(
    cfg: Config,
    fs_name: str,
    subvol_name: str,
    group_name: Optional[str] = None,
    force: bool = False,
) -> Dict[str, object]:
    """
    Delete a subvolume from every cluster (best effort).
    Returns which clusters deleted it, which didn't have it, and any errors.
    """
    logger = logging.getLogger(cfg.logging.logger)
    clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                      for name, entry in cfg.cluster.items()}

    deleted_from: List[str] = []
    not_found: List[str] = []
    errors: Dict[str, str] = {}

    for name, dc in clients.items():
        try:
            if dc.subvolume_exists(fs_name, subvol_name, group_name):
                dc.delete_subvolume(fs_name, subvol_name, group_name, force=force)
                deleted_from.append(name)
            else:
                not_found.append(name)
        except Exception as e:
            logger.error("Subvolume %s with group %s could not be deleted on %s at cluster %s",
                         subvol_name, group_name, fs_name, name)
            errors[name] = str(e)

    return SubvolDeleteResult(
        fs_name=fs_name,
        group_name=group_name,
        subvol_name=subvol_name,
        deleted_from=deleted_from,
        not_found=not_found,
        errors=errors,
    ).to_dict()
