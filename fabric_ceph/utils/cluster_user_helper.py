# fabric_ceph/utils/cluster_user_helper.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from http.client import BAD_REQUEST
from typing import Any, Dict, List, Optional

from fabric_ceph.common.config import Config
from fabric_ceph.response.ceph_exception import CephException
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.keyring_parser import keyring_minimal
import re
from typing import Tuple

# ---------- helpers (unchanged) ----------

# Stronger > weaker
_PERM_ORDER = {"r": 0, "rw": 1, "rwps": 2}

def _unescape_keyring_blob(s: str) -> str:
    # export sometimes returns a JSON-escaped string; unescape if needed
    s = str(s)
    if (s.startswith('"') and s.endswith('"')) or ('\\n' in s or '\\"' in s):
        try:
            return json.loads(s)
        except Exception:
            return s
    return s

def _extract_caps_by_entity_from_keyring(keyring_text: str) -> Dict[str, str]:
    """Pull raw mon/mds/osd cap strings from a keyring blob."""
    out: Dict[str, str] = {}
    if not isinstance(keyring_text, str):
        try:
            keyring_text = keyring_text.decode("utf-8", "ignore")
        except Exception:
            keyring_text = str(keyring_text)
    for ent in ("mon", "mds", "osd"):
        m = re.search(rf'caps\s+{ent}\s*=\s*"([^"]+)"', keyring_text)
        if m:
            out[ent] = m.group(1)
    return out

def _parse_mds_caps(mds: str) -> List[Tuple[str, str, str]]:
    """Return list of (fsname, path, perm)."""
    res: List[Tuple[str, str, str]] = []
    if not mds:
        return res
    for clause in (c.strip() for c in mds.split(",") if c.strip()):
        pm = re.search(r'allow\s+([a-z*]+)', clause)
        fm = re.search(r'fsname=([A-Za-z0-9_.:-]+)', clause)
        pa = re.search(r'path=([^,\s]+)', clause)
        if pm and fm and pa:
            res.append((fm.group(1), pa.group(1), pm.group(1)))
    return res

def _format_mds_caps(clauses: List[Tuple[str, str, str]]) -> str:
    """Choose strongest perm per (fs,path) and produce one MDS caps string."""
    best: Dict[Tuple[str, str], str] = {}
    for fs, path, perm in clauses:
        cur = best.get((fs, path))
        if cur is None or _PERM_ORDER.get(perm, 0) > _PERM_ORDER.get(cur, 0):
            best[(fs, path)] = perm
    ordered = sorted(best.items(), key=lambda kv: (kv[0][0], kv[0][1]))
    return ", ".join(f"allow {perm} fsname={fs} path={path}" for (fs, path), perm in ordered)

def _merge_mon(existing: str, new: str) -> str:
    fs_re = re.compile(r'fsname=([A-Za-z0-9_.:-]+)')
    have = set(fs_re.findall(existing or ""))
    add  = set(fs_re.findall(new or ""))
    allfs = have | add
    if not allfs:
        return existing or new
    return ", ".join(sorted({f"allow r fsname={fs}" for fs in allfs}))

def _merge_osd(existing: str, new: str) -> str:
    d_re = re.compile(r'data=([A-Za-z0-9_.:-]+)')
    m_re = re.compile(r'metadata=([A-Za-z0-9_.:-]+)')
    have_d, have_m = set(d_re.findall(existing or "")), set(m_re.findall(existing or ""))
    add_d,  add_m  = set(d_re.findall(new or "")),     set(m_re.findall(new or ""))
    all_d,  all_m  = have_d | add_d,                   have_m | add_m
    parts = {f"allow rw tag cephfs data={fs}" for fs in all_d}
    parts |= {f"allow rw tag cephfs metadata={fs}" for fs in all_m}
    return ", ".join(sorted(parts))


def _resolve_subvol_path(dc: DashClient, fs_name: str, subvol_name: str, group_name: Optional[str]) -> str:
    info = dc.get_subvolume_info(fs_name, subvol_name, group_name)
    for k in ("path", "full_path", "mount_path", "mountpoint"):
        p = info.get(k)
        if isinstance(p, str) and p.startswith("/"):
            return p
    for v in info.values():
        if isinstance(v, str) and v.startswith("/"):
            return v
    raise RuntimeError(
        f"Could not resolve subvolume path for {fs_name}:{group_name}:{subvol_name} on {dc.cluster_name}"
    )


def _merge_rendered_caps_per_entity(rendered_caps: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Take many rendered capability entries (possibly multiple per entity) and
    merge into one string per entity by comma-joining unique clauses.
    """
    per_entity: Dict[str, List[str]] = {}
    for item in rendered_caps:
        entity = item["entity"]
        cap = item["cap"].strip()
        per_entity.setdefault(entity, []).append(cap)

    merged: List[Dict[str, str]] = []
    for entity, clauses in per_entity.items():
        uniq = list(dict.fromkeys(clauses))  # de-duplicate, keep order
        merged.append({"entity": entity, "cap": ", ".join(uniq)})
    return merged


def _render_caps_for_contexts(tmpl_caps: List[Dict[str, str]], contexts: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Render template caps for each context, return the merged list (per-entity).
    contexts entries must contain keys: fs, path, group, subvol.
    """
    rendered_all: List[Dict[str, str]] = []
    for ctx in contexts:
        subs = {
            "fs": ctx["fs"],
            "path": ctx["path"],
            "group": ctx.get("group", "") or "",
            "subvol": ctx["subvol"],
        }
        for c in tmpl_caps:
            rendered_all.append({"entity": c["entity"], "cap": c["cap"].format(**subs)})
    return _merge_rendered_caps_per_entity(rendered_all)


# ---------- per-cluster user upsert (multi-context renders) ----------

def ensure_user_on_cluster_with_cluster_paths_multi(
    cfg: Config,
    cluster: str,
    user_entity: str,
    base_capabilities: List[Dict[str, str]],
    *,
    renders: List[Dict[str, str]],  # [{fs_name, subvol_name, group_name?}, ...]
) -> Dict[str, Any]:
    """
    Per-cluster version:
      - Resolve PATH for every render context on the given cluster.
      - Merge all rendered caps per entity (comma-join, de-dup).
      - Update user caps; on failure to update, create user with caps.
      - No secret export/import; strictly operates on this cluster.
    """
    log = logging.getLogger(cfg.logging.logger)

    if cluster not in cfg.cluster:
        raise CephException("Unknown cluster '%s'" % cluster, http_error_code=BAD_REQUEST)

    dc = DashClient.for_cluster(cluster, cfg.cluster[cluster])
    errors: Dict[str, str] = {}
    paths_first: Dict[str, str] = {}
    created_on_source = False
    updated_on_source = False
    caps_applied: Dict[str, List[Dict[str, str]]] = {}

    # Resolve all contexts on this cluster
    contexts: List[Dict[str, str]] = []
    try:
        for r in renders:
            p = _resolve_subvol_path(dc, r["fs_name"], r["subvol_name"], r.get("group_name"))
            contexts.append(
                {
                    "fs": r["fs_name"],
                    "subvol": r["subvol_name"],
                    "group": r.get("group_name") or "",
                    "path": p,
                }
            )
        if contexts:
            paths_first[cluster] = contexts[0]["path"]
    except Exception as e:
        err = f"path resolution failed: {e}"
        log.exception(err)
        errors[cluster] = err

    try:
        # Render request -> per-entity strings (already merged across renders)
        caps_here = _render_caps_for_contexts(base_capabilities, contexts)
        new_map = {c["entity"]: c["cap"] for c in caps_here}

        # Read existing caps from keyring (if user exists)
        try:
            existing_keyring = dc.export_keyring(user_entity)
            existing_keyring = _unescape_keyring_blob(existing_keyring)
            existing_map = _extract_caps_by_entity_from_keyring(existing_keyring) if existing_keyring else {}
            log.debug(f"existing keyring: {existing_keyring}")
        except Exception:
            existing_map = {}

        # Merge (no downgrade): mds union by (fs,path) with stronger permission; mon/osd union by fs
        ex = _parse_mds_caps(existing_map.get("mds", ""))
        log.debug(f"existing mds parsed: {ex}")
        nw = _parse_mds_caps(new_map.get("mds", ""))
        log.debug(f"new mds parsed: {nw}")
        merged_mds = _format_mds_caps(
            ex + nw
        ) if ("mds" in new_map or "mds" in existing_map) else ""

        merged_mon = _merge_mon(existing_map.get("mon", ""), new_map.get("mon", "")) \
            if ("mon" in new_map or "mon" in existing_map) else ""

        merged_osd = _merge_osd(existing_map.get("osd", ""), new_map.get("osd", "")) \
            if ("osd" in new_map or "osd" in existing_map) else ""

        log.debug(f"new mds: {new_map}")
        log.debug(f"merged mon: {merged_mon}")
        log.debug(f"merged mds: {merged_mds}")
        log.debug(f"merged osd: {merged_osd}")

        # Anything else → simple comma-union (rare)
        merged_other: Dict[str, str] = {}
        for ent, cap in new_map.items():
            if ent in ("mds", "mon", "osd"):
                continue
            prev = existing_map.get(ent, "")
            merged_other[ent] = ", ".join(dict.fromkeys(
                [s.strip() for s in (prev.split(",") if prev else [])] +
                [s.strip() for s in (cap.split(",") if cap else [])]
            ))

        final_caps: List[Dict[str, str]] = []
        if merged_mon: final_caps.append({"entity": "mon", "cap": merged_mon})
        if merged_mds: final_caps.append({"entity": "mds", "cap": merged_mds})
        if merged_osd: final_caps.append({"entity": "osd", "cap": merged_osd})
        for ent, cap in merged_other.items():
            if cap:
                final_caps.append({"entity": ent, "cap": cap})

        log.debug(f"final caps: {final_caps}")
        # Apply (update → create fallback)
        status = dc.update_user_caps(user_entity, final_caps)
        if status in (200, 201, 202):
            updated_on_source = True
        else:
            dc.create_user(user_entity, final_caps)
            created_on_source = True
            updated_on_source = True

        caps_applied[cluster] = final_caps
    except Exception as e:
        err = f"apply failed: {e}"
        log.exception(err)
        errors[cluster] = err

    return {
        "user_entity": user_entity,
        "source_cluster": cluster,        # kept for schema compatibility
        "created_on_source": created_on_source,
        "updated_on_source": updated_on_source,
        "imported_to": [],                # none in per-cluster mode
        "caps_applied": caps_applied,     # { cluster: [ {entity, cap}, ... ] }
        "paths": paths_first,             # { cluster: "<first path>" }
        "errors": errors,
    }


# ---------- per-cluster delete ----------

@dataclass
class DeleteResult:
    entity: str
    deleted_from: List[str]
    not_found: List[str]
    errors: Dict[str, str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "entity": self.entity,
            "deleted_from": self.deleted_from,
            "not_found": self.not_found,
            "errors": self.errors,
        }


def delete_user_on_cluster(
    cfg: Config,
    cluster: str,
    user_entity: str,
) -> Dict[str, object]:
    """
    Delete `user_entity` on a single cluster.
    """
    logger = logging.getLogger(cfg.logging.logger)

    if cluster not in cfg.cluster:
        return DeleteResult(
            entity=user_entity, deleted_from=[], not_found=[], errors={cluster: f"unknown cluster '{cluster}'"}
        ).to_dict()

    dc = DashClient.for_cluster(cluster, cfg.cluster[cluster])

    deleted_from: List[str] = []
    not_found: List[str] = []
    errors: Dict[str, str] = {}

    try:
        ok, _msg = dc.delete_user(user_entity)
        if ok:
            deleted_from.append(cluster)
        else:
            not_found.append(cluster)
    except Exception as e:
        logger.exception("Encountered exception while deleting %s on %s", user_entity, cluster)
        errors[cluster] = str(e)

    return DeleteResult(
        entity=user_entity, deleted_from=deleted_from, not_found=not_found, errors=errors
    ).to_dict()


# ---------- per-cluster list/export ----------

def list_users_on_cluster(cfg: Config, cluster: str) -> Dict[str, object]:
    """
    Return users from the specified cluster.

    Returns:
        {
          "cluster": "<name>",
          "users": [ {...}, {...}, ... ]   # raw Dashboard objects
        }
    """
    logger = logging.getLogger(cfg.logging.logger)

    if cluster not in cfg.cluster:
        raise RuntimeError(f"unknown cluster '{cluster}'")

    dc = DashClient.for_cluster(cluster, cfg.cluster[cluster])
    try:
        users = dc.list_users()
        return {"cluster": cluster, "users": users}
    except Exception as e:
        logger.exception("Encountered exception while fetching users on %s", cluster)
        raise RuntimeError(f"list_users failed on {cluster}: {e}") from e


def export_users_on_cluster(
    cfg: Config,
    cluster: str,
    entities: List[str],
    keyring_only: bool,
) -> Dict[str, object]:
    """
    Export keyring(s) for specific entities from a single cluster.

    Returns:
        {
          "cluster": "<name>",
          "entities": { "<entity>": "<keyring or key>" , ... }
        }
    """
    logger = logging.getLogger(cfg.logging.logger)

    if not entities:
        raise ValueError("entities must be a non-empty list")

    if cluster not in cfg.cluster:
        raise RuntimeError(f"unknown cluster '{cluster}'")

    dc = DashClient.for_cluster(cluster, cfg.cluster[cluster])
    exported: Dict[str, str] = {}

    for ent in entities:
        try:
            keyring = dc.export_keyring(ent)
            exported[ent] = keyring if not keyring_only else (keyring_minimal(keyring) or "")
            if keyring_only and not exported[ent]:
                raise RuntimeError("key not found in exported keyring")
        except Exception as e:
            logger.exception("Encountered exception while exporting %s on %s", ent, cluster)

    # Optionally, you could filter out failures:
    # exported = {k: v for k, v in exported.items() if v}

    return {"cluster": cluster, "entities": exported}


if __name__ == "__main__":
    existing = "[client.kthare10_0011904101]\n\tkey = AQDJN+VoeZugGBAABb4p6+y42l5o7vFDUS0FIg==\n\tcaps mds = \"allow rw fsname=CEPH-FS-01 path=/volumes/_nogroup/kthare10_0011904101/8a648e13-6e39-47b3-8245-d05a702aeb00, allow rw fsname=CEPH-FS-01 path=/volumes/04b14c17-e66a-4405-98fc-d737717e2160/fabric-staff-no-permissions/c1431649-2b31-4a4b-8e62-2b71279433a3\"\n\tcaps mon = \"allow r fsname=CEPH-FS-01\"\n\tcaps osd = \"allow rw tag cephfs data=CEPH-FS-01, allow rw tag cephfs metadata=CEPH-FS-01\"\n\n"
    existing_map = _extract_caps_by_entity_from_keyring(existing)
    new_mds = 'allow rw fsname=CEPH-FS-01 path=/volumes/04b14c17-e66a-4405-98fc-d737717e2160/fabric-staff-no-permissions/c1431649-2b31-4a4b-8e62-2b71279433a3'
    merge = _format_mds_caps(_parse_mds_caps(existing_map.get("mds", "")) +
            _parse_mds_caps(new_mds))
    print(merge)
    print(type(merge))