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
from typing import Dict, List, Optional, Tuple

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

# --- helpers ---------------------------------------------------------------

def _extract_path_from_info(info: Dict[str, object]) -> Optional[str]:
    """
    Try a few common keys used by Ceph mgr/cephfs subvolume APIs to get the
    canonical mount path of a subvolume.
    """
    # Common fields seen from ceph 'fs subvolume getpath' or mgr REST:
    for k in ("path", "mount_path", "mountpoint", "subvol_path"):
        v = info.get(k)
        if isinstance(v, str) and v.startswith("/"):
            return v
    return None


def _split_mds_clauses(mds_caps: str) -> List[str]:
    """
    Split MDS caps string into comma-separated clauses, trimming whitespace.
    Example input:
      'allow rw fsname=CEPH-FS-01 path=/vol/a, allow r fsname=CEPH-FS-01 path=/vol/b'
    -> ['allow rw fsname=CEPH-FS-01 path=/vol/a', 'allow r fsname=CEPH-FS-01 path=/vol/b']
    """
    return [c.strip() for c in mds_caps.split(",") if c.strip()]


def _join_mds_clauses(clauses: List[str]) -> str:
    return ", ".join(clauses)


def _remove_exact_path_clause_from_mds_caps(
    mds_caps: str, *, fs_name: str, target_path: str
) -> Tuple[str, bool]:
    """
    Remove ONLY the clause(s) that exactly match fs_name AND target_path.
    Returns (new_caps, changed_flag).
    """
    if not mds_caps.strip():
        return mds_caps, False

    clauses = _split_mds_clauses(mds_caps)
    keep: List[str] = []
    changed = False

    # Match fsname and path exactly within a clause.
    # We don't try to be too clever here—just require both tokens present.
    fs_token = f"fsname={fs_name}"
    path_token = f"path={target_path}"

    for c in clauses:
        has_fs = fs_token in c
        has_path = path_token in c
        if has_fs and has_path:
            changed = True
            # skip (i.e., remove) this clause
            continue
        keep.append(c)

    new_caps = _join_mds_clauses(keep)
    return new_caps, changed


def _auth_list_expected_shape_note() -> str:
    return (
        "Expected DashClient.auth_list() to return a list of entries like:\n"
        "[\n"
        "  {\n"
        "    'entity': 'client.username',\n"
        "    'caps': {'mon': '...', 'osd': '...', 'mds': '...'}\n"
        "  },\n"
        "  ...\n"
        "]"
    )


def _revoke_caps_for_path(
    logger: logging.Logger,
    dc,
    *,
    fs_name: str,
    target_path: str,
    dry_run: bool = False,
) -> List[str]:
    """
    Iterate all client principals and remove the mds cap clause for (fs_name, target_path).
    Returns list of principals modified.
    """
    principals_modified: List[str] = []

    # You may already have equivalent methods; adjust names if needed.
    # - dc.auth_list() -> list of {'entity': 'client.foo', 'caps': {'mds': '...', 'mon': '...', 'osd': '...'}}
    # - dc.auth_caps_set(entity, mds=<new>) -> updates only mds caps (preferred),
    #   or dc.set_client_caps(entity, mds=<new>, mon=<existing>, osd=<existing>)
    auth_entries = dc.auth_list()
    if not isinstance(auth_entries, list):
        logger.error("auth_list() did not return a list. %s", _auth_list_expected_shape_note())
        return principals_modified

    for ent in auth_entries:
        try:
            entity = ent.get("entity", "")
            if not entity.startswith("client."):
                continue

            caps = ent.get("caps", {})
            mds_caps = caps.get("mds", "") or ""
            if not mds_caps:
                continue

            new_mds, changed = _remove_exact_path_clause_from_mds_caps(
                mds_caps, fs_name=fs_name, target_path=target_path
            )
            if not changed:
                continue

            # If no clauses remain, set empty mds caps (equivalent to no MDS permission).
            if not new_mds.strip():
                new_mds = ""

            logger.info("Revoking MDS path cap for %s: path=%s (fs=%s)", entity, target_path, fs_name)
            logger.debug("Old mds: %r", mds_caps)
            logger.debug("New mds: %r", new_mds)

            if not dry_run:
                # Prefer a method that updates just the mds caps to avoid accidental broadening.
                if hasattr(dc, "auth_caps_set"):
                    dc.auth_caps_set(entity,
                                     mds=new_mds,
                                     mon=caps.get("mon", ""),
                                     osd=caps.get("osd", ""),)
                elif hasattr(dc, "set_client_caps"):
                    # Fall back to setting all, preserving existing mon/osd caps.
                    dc.set_client_caps(
                        entity,
                        mds=new_mds,
                        mon=caps.get("mon", ""),
                        osd=caps.get("osd", ""),
                    )
                else:
                    raise RuntimeError(
                        "DashClient has neither auth_caps_set() nor set_client_caps()."
                    )

            principals_modified.append(entity)

        except Exception as e:
            logger.exception("Failed to update caps for %s: %s", ent, e)

    return principals_modified

def delete_subvolume_on_cluster(
    cfg: "Config",
    cluster: str,
    fs_name: str,
    subvol_name: str,
    group_name: Optional[str] = None,
    force: bool = False,
    revoke_caps: bool = True,
    dry_run: bool = False,   # set True to see who would be modified without changing caps
) -> Dict[str, object]:
    """
    Delete a subvolume from ONE cluster (best effort).
    Additionally (optional), remove MDS path capabilities pointing to this subvolume path
    from all CephFS client users.

    Returns SubvolDeleteResult with single-element lists,
    plus 'caps_revoked_for' (list of client principals).
    """
    logger = logging.getLogger(cfg.logging.logger)

    if cluster not in cfg.cluster:
        raise ValueError(f"Unknown cluster '{cluster}'")

    entry = cfg.cluster[cluster]
    dc = DashClient.for_cluster(cluster, entry)

    deleted_from: List[str] = []
    not_found: List[str] = []
    errors: Dict[str, str] = {}
    caps_revoked_for: List[str] = []

    # 1) Resolve canonical path before deletion (so we can revoke against the exact path).
    target_path: Optional[str] = None
    try:
        if dc.subvolume_exists(fs_name, subvol_name, group_name):
            try:
                info = dc.get_subvolume_info(fs_name, subvol_name, group_name)
            except Exception:
                info = {}
                logger.debug("get_subvolume_info failed; will attempt best-effort path resolution.")
            target_path = _extract_path_from_info(info)

            # If we couldn't get the canonical path, we *could* try best-effort guessing.
            # It's safer to skip revocation than to guess wrong and over-revoke.
            if not target_path:
                logger.warning(
                    "Could not determine canonical path for subvolume %s (group=%s) on %s (fs=%s); "
                    "cap revocation will be skipped.",
                    subvol_name, group_name, cluster, fs_name
                )

            # 2) Delete subvolume
            dc.delete_subvolume(fs_name, subvol_name, group_name, force=force)
            deleted_from.append(cluster)

            # 3) Revoke caps that referenced this exact path
            if revoke_caps and target_path:
                try:
                    caps_revoked_for = _revoke_caps_for_path(
                        logger, dc, fs_name=fs_name, target_path=target_path, dry_run=dry_run
                    )
                except Exception as e:
                    logger.error(
                        "Cap revocation failed after deleting %s (path=%s) on cluster %s (fs=%s)",
                        subvol_name, target_path, cluster, fs_name
                    )
                    logger.exception(e)
                    errors[f"{cluster}::cap-revoke"] = str(e)
        else:
            not_found.append(cluster)
    except Exception as e:
        logger.error(
            "Subvolume %s (group=%s) could not be deleted on cluster %s (fs=%s)",
            subvol_name, group_name, cluster, fs_name
        )
        logger.exception(e)
        errors[cluster] = str(e)

    result = SubvolDeleteResult(
        fs_name=fs_name,
        group_name=group_name,
        subvol_name=subvol_name,
        deleted_from=deleted_from,
        not_found=not_found,
        errors=errors,
    ).to_dict()

    # Add extra diagnostics without breaking existing consumers.
    result["caps_revoked_for"] = caps_revoked_for
    result["dry_run"] = dry_run
    return result