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

# ---------- single-cluster operations ----------

def ensure_subvolume_on_cluster(
    cfg: Config,
    cluster: str,
    fs_name: str,
    subvol_name: str,
    group_name: Optional[str] = None,
    size_bytes: Optional[int] = None,
    mode: Optional[str] = None,
) -> Dict[str, object]:
    """
    Ensure a CephFS subvolume (and optional group) exists on ONE cluster and apply 'size_bytes' (quota) if provided.

    Returns SubvolSyncResult (maps contain a single key = the target cluster).
    """
    logger = logging.getLogger(cfg.logging.logger)

    if cluster not in cfg.cluster:
        raise ValueError(f"Unknown cluster '{cluster}'")

    entry = cfg.cluster[cluster]
    dc = DashClient.for_cluster(cluster, entry)

    applied: Dict[str, str] = {}
    paths: Dict[str, str] = {}
    errors: Dict[str, str] = {}

    existed_on_source = False
    created_on_source = False

    try:
        # Ensure group if requested
        if group_name:
            dc.ensure_subvol_group(fs_name, group_name)

        # Existence check
        exists = dc.subvolume_exists(fs_name, subvol_name, group_name)
        existed_on_source = bool(exists)

        if exists:
            # Resize if a quota was specified; otherwise it's a no-op
            if size_bytes is not None and int(size_bytes) >= 0:
                dc.resize_subvolume(fs_name, subvol_name, group_name, size_bytes=size_bytes)
                applied[cluster] = "resized"
            else:
                applied[cluster] = "ok"
        else:
            # Create; pass mode if provided; omit size for unlimited
            dc.create_subvolume(
                fs_name, subvol_name, group_name, size_bytes=size_bytes, mode=mode
            )
            applied[cluster] = "created"
            created_on_source = True

        # Fetch info to capture path
        info = dc.get_subvolume_info(fs_name, subvol_name, group_name)
        spath = None
        for k in ("path", "full_path", "mount_path", "mountpoint"):
            v = info.get(k) if isinstance(info, dict) else None
            if isinstance(v, str) and v.startswith("/"):
                spath = v
                break
        if not spath and isinstance(info, dict):
            spath = next((v for v in info.values() if isinstance(v, str) and v.startswith("/")), "")
        if spath:
            paths[cluster] = spath

    except Exception as e:
        logger.error(
            "Subvolume %s (group=%s) could not be ensured on cluster %s (fs=%s)",
            subvol_name, group_name, cluster, fs_name
        )
        logger.exception(e)
        errors[cluster] = str(e)

    return SubvolSyncResult(
        fs_name=fs_name,
        group_name=group_name,
        subvol_name=subvol_name,
        requested_size=(int(size_bytes) if size_bytes is not None else None),
        requested_mode=(str(mode) if mode else None),
        source_cluster=cluster,
        existed_on_source=existed_on_source,
        created_on_source=created_on_source,
        applied=applied,
        paths=paths,
        errors=errors,
    ).to_dict()


def delete_subvolume_on_cluster(
    cfg: Config,
    cluster: str,
    fs_name: str,
    subvol_name: str,
    group_name: Optional[str] = None,
    force: bool = False,
) -> Dict[str, object]:
    """
    Delete a subvolume from ONE cluster (best effort).

    Returns SubvolDeleteResult with single-element lists.
    """
    logger = logging.getLogger(cfg.logging.logger)

    if cluster not in cfg.cluster:
        raise ValueError(f"Unknown cluster '{cluster}'")

    entry = cfg.cluster[cluster]
    dc = DashClient.for_cluster(cluster, entry)

    deleted_from: List[str] = []
    not_found: List[str] = []
    errors: Dict[str, str] = {}

    try:
        if dc.subvolume_exists(fs_name, subvol_name, group_name):
            dc.delete_subvolume(fs_name, subvol_name, group_name, force=force)
            deleted_from.append(cluster)
        else:
            not_found.append(cluster)
    except Exception as e:
        logger.error(
            "Subvolume %s (group=%s) could not be deleted on cluster %s (fs=%s)",
            subvol_name, group_name, cluster, fs_name
        )
        logger.exception(e)
        errors[cluster] = str(e)

    return SubvolDeleteResult(
        fs_name=fs_name,
        group_name=group_name,
        subvol_name=subvol_name,
        deleted_from=deleted_from,
        not_found=not_found,
        errors=errors,
    ).to_dict()
