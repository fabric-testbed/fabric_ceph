# ceph_subvol_gui.py
"""
FABRIC CephFS Manager — 2-Tab GUI
---------------------------------
Tabs:
  1) Subvolumes & Groups: create/resize, browse (by group or all), apply/revoke caps
     (single user or project), delete subvols, delete/empty groups
  2) Users: list/search CephX users, export keyrings, delete users

Notes
-----
- Uses token_file (JSON with 'id_token') for auth.
- Cluster dropdown is auto-populated via /cluster/info.
- Apply Caps sends placeholder caps and requests server-side merge ("multi"):
    mon: allow r fsname={fs}
    mds: allow rw fsname={fs} path={path}
    osd: allow rw tag cephfs data={fs}
    osd: allow rw tag cephfs metadata={fs}

NEW
---
- Revoke caps: removes the MDS path clause for the selected volume from a user or all users in a project.
- Works for grouped and ungrouped subvolumes (group=None).
- Uses Reports API to load ACTIVE users/projects for the pickers.
- Uses Core API ONLY to resolve project membership (members' bastion_login) when applying/revoking caps to a project.
"""

from __future__ import annotations

import datetime
import os
import json
from typing import Optional, List, Dict, Any, Tuple, Iterable

# -------- Defaults (override in notebook or via env) --------
DEFAULT_CEPH_API_BASE: Optional[str] = os.getenv("FABRIC_CEPH_API_BASE")
DEFAULT_REPORTS_API_BASE: Optional[str] = os.getenv("FABRIC_REPORTS_API_BASE")
DEFAULT_CORE_API_BASE: Optional[str] = os.getenv("FABRIC_CORE_API_BASE")
DEFAULT_VERIFY_TLS: bool = os.getenv("FABRIC_VERIFY_TLS", "true").lower() in {"1", "true", "yes", "on"}
DEFAULT_CEPHFS_VOLUME: str = os.getenv("FABRIC_CEPHFS_VOLUME", "cephfs")
DEFAULT_CLUSTER: Optional[str] = os.getenv("FABRIC_CEPH_CLUSTER")
DEFAULT_TOKEN_FILE: Optional[str] = os.getenv("FABRIC_TOKEN_FILE")

# -------- Dynamic imports --------
def _import_ceph_client():
    try:
        from fabric_ceph_client import CephManagerClient  # local fallback
        return CephManagerClient, None
    except Exception as e1:
        try:
            from fabric_ceph_client.fabric_ceph_client import CephManagerClient  # packaged
            return CephManagerClient, None
        except Exception as e2:
            return None, f"{e1!r}; {e2!r}"

def _import_reports_client():
    try:
        from reports_api import ReportsApi  # local fallback
        return ReportsApi, None
    except Exception as e1:
        try:
            from fabric_reports_client.reports_api import ReportsApi  # packaged
            return ReportsApi, None
        except Exception as e2:
            return None, f"{e1!r}; {e2!r}"

def _import_core_api():
    try:
        from core_api import CoreApi
        return CoreApi, None
    except Exception as e:
        return None, repr(e)

# -------- Helpers --------
def _slugify_subvol_from_project(p: dict) -> str:
    import re
    base = (p.get("project_name") or p.get("project_id") or "project").lower()
    base = re.sub(r"[^a-z0-9\-]+", "-", base).strip("-")
    base = re.sub(r"-{2,}", "-", base)
    if not base:
        base = (p.get("project_id") or "project")[:12].lower()
    return base[:63]

def _bytes_from_gib(gib: int) -> int:
    return int(gib) * 1024**3

def _extract_users(rows: Any) -> List[Dict[str, Any]]:
    """
    Normalize and extract unique user dicts from an API-style response.

    Accepts dict/list/None. A "user" has at least one of: bastion_login, user_email, user_id.
    Dedup by (bastion_login || user_email || user_id); sort by bastion_login, user_email, user_id.
    """
    if isinstance(rows, dict):
        rows = rows.get("data", [])
    if rows is None:
        rows = []
    if not isinstance(rows, Iterable) or isinstance(rows, (str, bytes)):
        rows = [rows]

    candidates: List[Dict[str, Any]] = []
    for r in rows:
        if isinstance(r, dict) and ("bastion_login" in r or "user_email" in r or "user_id" in r):
            candidates.append(r)

    seen = set()
    unique: List[Dict[str, Any]] = []
    for r in candidates:
        key = r.get("bastion_login") or r.get("user_email") or r.get("user_id")
        if key and key not in seen:
            seen.add(key)
            unique.append(r)

    def sort_key(d: Dict[str, Any]):
        bl = d.get("bastion_login")
        em = d.get("user_email")
        uid = d.get("user_id")
        return (
            (str(bl).lower() if bl is not None else ""),
            (str(em).lower() if em is not None else ""),
            (str(uid).lower() if uid is not None else ""),
        )

    unique.sort(key=sort_key)
    return unique

def _extract_projects(rows: Any) -> List[Dict[str, Any]]:
    """
    Normalize and extract unique project dicts from an API-style response.
    Keep only dicts with "project_id". Dedup by project_id. Sort by project_name (fallback id).
    """
    if isinstance(rows, dict):
        rows = rows.get("data", [])
    if rows is None:
        rows = []
    if not isinstance(rows, Iterable) or isinstance(rows, (str, bytes)):
        rows = [rows]

    candidates: List[Dict[str, Any]] = []
    for r in rows:
        if isinstance(r, dict) and "project_id" in r:
            candidates.append(r)

    seen: set = set()
    unique: List[Dict[str, Any]] = []
    for r in candidates:
        pid = r.get("project_id")
        if pid and pid not in seen:
            seen.add(pid)
            unique.append(r)

    def sort_key(d: Dict[str, Any]) -> tuple:
        name = d.get("project_name")
        pid = d.get("project_id")
        return ((str(name).lower() if name is not None else ""), (str(pid) if pid is not None else ""))

    unique.sort(key=sort_key)
    return unique

def _user_label(u: Dict[str, Any]) -> str:
    bl = u.get("bastion_login") or ""
    em = u.get("user_email") or ""
    return f"{bl or em} | {em}" if bl and em else (bl or em or u.get("user_id", "<unknown>"))

def _project_label(p: Dict[str, Any]) -> str:
    return f"{p.get('project_name') or p.get('project_id')} [{p.get('project_id')}]"

# --- Keyring / caps parsing helpers (for revoke) ---
def _extract_caps_by_entity_from_keyring_text(keyring_text: str) -> Dict[str, str]:
    import re
    caps: Dict[str, str] = {}
    for ent in ("mds", "mon", "osd", "mgr"):
        m = re.search(rf'caps\s+{ent}\s*=\s*"([^"]*)"', keyring_text)
        if m:
            caps[ent] = m.group(1)
    return caps

def _remove_exact_path_clause_from_mds_caps(mds_caps: str, fs_name: str, target_path: str) -> Tuple[str, bool]:
    if not (mds_caps and fs_name and target_path):
        return mds_caps or "", False
    parts = [p.strip() for p in mds_caps.split(",") if p.strip()]
    keep: List[str] = []
    changed = False
    for clause in parts:
        lc = clause.lower().replace("  ", " ")
        if ("fsname=" in lc) and ("path=" in lc) and (f"fsname={fs_name.lower()}" in lc) and (f"path={target_path.lower()}" in lc):
            changed = True
            continue
        keep.append(clause)
    return (", ".join(keep), changed)

def _deep_find_path(obj: Any) -> Optional[str]:
    """Search for first 'path' key in nested dict/list structures."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "path" and isinstance(v, str):
                return v
            found = _deep_find_path(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _deep_find_path(item)
            if found:
                return found
    return None

# -------- GUI --------
def launch():
    import ipywidgets as W
    from IPython.display import display, clear_output

    CephClient, ceph_err = _import_ceph_client()
    ReportsClient, reports_err = _import_reports_client()
    CoreApiClass, core_err = _import_core_api()

    # ---------- Shared connection widgets ----------
    ceph_url = W.Text(description="Ceph API", value=(DEFAULT_CEPH_API_BASE or ""), placeholder="https://mgr/api", layout=W.Layout(width="55%"))
    reports_url = W.Text(description="Reports API", value=(DEFAULT_REPORTS_API_BASE or ""), placeholder="https://reports/api", layout=W.Layout(width="55%"))
    core_api_url = W.Text(description="Core API", value=(DEFAULT_CORE_API_BASE or ""), placeholder="https://uis.fabric-testbed.net", layout=W.Layout(width="55%"))
    token_file = W.Text(description="Token File", value=(DEFAULT_TOKEN_FILE or ""), placeholder="Path to JSON with id_token")
    verify = W.Checkbox(value=DEFAULT_VERIFY_TLS, description="Verify TLS")
    cluster_dd = W.Dropdown(description="Cluster", options=[], disabled=True, layout=W.Layout(width="25%"))
    vol_name = W.Text(description="FS Volume", value=DEFAULT_CEPHFS_VOLUME, placeholder="CephFS vol_name (e.g., cephfs)")
    connect = W.Button(description="Connect", button_style="primary")

    # ---------- SUBVOLS & GROUPS TAB ----------
    scope = W.ToggleButtons(options=[("Per-User", "user"), ("Per-Project", "project")], description="Create for")
    project_dd = W.Dropdown(description="Project", options=[], disabled=True)
    user_dd = W.Dropdown(description="User", options=[], disabled=False)
    name = W.Text(description="Subvol Name", placeholder="Per-user: auto (bastion). Per-project: required.")
    size_gib = W.BoundedIntText(description="Size (GiB)", min=1, max=1024*1024, value=10)
    create_btn = W.Button(description="Create/Resize", button_style="success")

    # Browse by group
    groups_dd = W.Dropdown(description="Group", options=[], disabled=True)
    refresh_groups_btn = W.Button(description="Refresh Groups")
    subvols_dd = W.Dropdown(description="Subvol (in Group)", options=[], disabled=True, layout=W.Layout(width="60%"))
    refresh_subvols_btn = W.Button(description="Refresh Subvols")
    info_sel_btn = W.Button(description="Info (Selected)")
    delete_sel_btn = W.Button(description="Delete (Selected)", button_style="danger")

    # Group-level deletes
    confirm_del_chk = W.Checkbox(value=False, description="Confirm deletions")
    delete_group_btn = W.Button(description="Delete Group (all subvols)", button_style="danger")
    remove_group_btn = W.Button(description="Remove Group (empty)", button_style="danger")  # best-effort

    # List all subvols (no group filter)
    all_subvols_sel = W.Select(description="All Subvols", options=[], rows=8, disabled=True, layout=W.Layout(width="60%"))
    refresh_all_btn = W.Button(description="Refresh All")
    info_all_btn = W.Button(description="Info (All→Selected)")
    delete_all_btn = W.Button(description="Delete (All→Selected)", button_style="danger")

    # Apply / Revoke caps to users or project
    apply_header = W.HTML("<b>Permissions</b><br><small>Pick a volume via <i>Group/Subvol</i> or <i>All Subvols</i> below, then choose targets.</small>")
    apply_selection_lbl = W.HTML("<i>No volume selected yet — pick from Group/Subvol or All Subvols.</i>")
    apply_target = W.ToggleButtons(options=[("Single User", "user"), ("Entire Project", "project")], description="Target")
    apply_user_dd = W.Dropdown(description="User", options=[], disabled=False)
    apply_project_dd = W.Dropdown(description="Project", options=[], disabled=True)
    apply_btn = W.Button(description="Apply Caps to Selected Volume", button_style="warning")
    revoke_btn = W.Button(description="Revoke Caps from Selected Volume", button_style="", tooltip="Removes the MDS path clause for the selected volume")

    # ---------- USERS TAB ----------
    users_filter = W.Text(description="Search", placeholder="entity contains...")
    users_sel = W.SelectMultiple(description="CephX Users", options=[], rows=12, disabled=True, layout=W.Layout(width="60%"))
    refresh_users_btn = W.Button(description="Refresh Users")
    export_users_btn = W.Button(description="Export Selected")
    delete_users_btn = W.Button(description="Delete Selected", button_style="danger")
    confirm_del_usr_chk = W.Checkbox(value=False, description="Confirm user deletions")

    # ---------- Output ----------
    status = W.HTML(value="")
    results = W.Output(layout={"border": "1px solid #eee", "min_height": "200px"})
    log = W.Output(layout={"border": "1px solid #ddd"})

    # ---------- State ----------
    ceph = None
    reports = None
    core_api = None
    users_cache: List[Dict[str, Any]] = []
    projects_cache: List[Dict[str, Any]] = []
    cephx_users_cache: List[Dict[str, Any]] = []
    all_subvols_cache: List[str] = []

    def _log(msg: str):
        from datetime import datetime
        with log:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    # ---------- Data loaders ----------
    def _populate_lists(_=None):
        nonlocal users_cache, projects_cache
        try:
            if reports:
                query_start = datetime.datetime.now(datetime.timezone.utc)
                users = reports.query_users(user_active=True, fetch_all=True, end_time=query_start)
                projects = reports.query_projects(project_active=True, fetch_all=True, end_time=query_start)
                users_cache = _extract_users(users)
                projects_cache = _extract_projects(projects)
                user_dd.options = [(_user_label(u), i) for i, u in enumerate(users_cache)]
                apply_user_dd.options = user_dd.options
                project_dd.options = [(_project_label(p), i) for i, p in enumerate(projects_cache)]
                apply_project_dd.options = project_dd.options
                _log(f"Loaded {len(users_cache)} users, {len(projects_cache)} projects from Reports.")
            else:
                user_dd.options = apply_user_dd.options = []
                project_dd.options = apply_project_dd.options = []
                users_cache, projects_cache = [], []
                _log("Reports API not connected; pickers are empty.")
        except Exception as e:
            _log(f"Failed to populate lists: {e}")

    def _populate_clusters():
        if ceph is None:
            return
        try:
            info = ceph.list_cluster_info()
            items = (info or {}).get("data", []) if isinstance(info, dict) else []
            names = [i.get("cluster") for i in items if isinstance(i, dict) and i.get("cluster")]
            if not names:
                raise ValueError("No clusters returned")
            cluster_dd.options = names
            cluster_dd.value = DEFAULT_CLUSTER if (DEFAULT_CLUSTER and DEFAULT_CLUSTER in names) else names[0]
            cluster_dd.disabled = False
            _log(f"Loaded clusters: {', '.join(names)}")
        except Exception as e:
            if DEFAULT_CLUSTER:
                cluster_dd.options = [DEFAULT_CLUSTER]; cluster_dd.value = DEFAULT_CLUSTER; cluster_dd.disabled = False
                _log(f"Cluster list failed; using default: {DEFAULT_CLUSTER} ({e})")
            else:
                cluster_dd.options = []; cluster_dd.disabled = True
                _log(f"Cluster list failed and no default set: {e}")

    # ---------- Helpers: groups/subvols ----------
    def _groups_from_resp(resp: Any) -> List[str]:
        data = (resp or {}).get("data", []) if isinstance(resp, dict) else []
        names: List[str] = []
        for g in data:
            if isinstance(g, str):
                names.append(g)
            elif isinstance(g, dict):
                nm = g.get("group_name") or g.get("name") or g.get("group") or ""
                if nm:
                    names.append(nm)
        return sorted(set(names))

    def _subvols_from_resp(resp: Any) -> List[str]:
        data = (resp or {}).get("data", []) if isinstance(resp, dict) else []
        names: List[str] = []
        for s in data:
            if isinstance(s, str):
                names.append(s)
            elif isinstance(s, dict):
                nm = s.get("name") or s.get("subvol_name") or s.get("id") or ""
                if nm:
                    names.append(nm)
        return sorted(set(names))

    def _refresh_groups(_=None):
        if ceph is None:
            return
        try:
            cl = (cluster_dd.value or "").strip()
            vol = (vol_name.value or DEFAULT_CEPHFS_VOLUME or "").strip()
            if not cl or not vol:
                return
            resp = ceph.list_subvolume_groups(cluster=cl, vol_name=vol, info=False)
            names = _groups_from_resp(resp)
            groups_dd.options = names; groups_dd.disabled = not bool(names)
            subvols_dd.options = []; subvols_dd.disabled = True
            _log(f"Loaded {len(names)} group(s).")
            _update_apply_selection_badge()
        except Exception as e:
            groups_dd.options = []; groups_dd.disabled = True
            _log(f"Refresh groups failed: {e}")
            _update_apply_selection_badge()

    def _refresh_subvols(_=None):
        if ceph is None:
            return
        try:
            cl = (cluster_dd.value or "").strip()
            vol = (vol_name.value or DEFAULT_CEPHFS_VOLUME or "").strip()
            grp = groups_dd.value
            if not cl or not vol or not grp:
                subvols_dd.options = []; subvols_dd.disabled = True
                _update_apply_selection_badge()
                return
            resp = ceph.list_subvolumes(cluster=cl, vol_name=vol, group_name=grp, info=False)
            names = _subvols_from_resp(resp)
            subvols_dd.options = names; subvols_dd.disabled = not bool(names)
            _log(f"Loaded {len(names)} subvolume(s) for group {grp}.")
        except Exception as e:
            subvols_dd.options = []; subvols_dd.disabled = True
            _log(f"Refresh subvols failed: {e}")
        finally:
            _update_apply_selection_badge()

    def _refresh_all_subvols(_=None):
        if ceph is None:
            return
        try:
            cl = (cluster_dd.value or "").strip()
            vol = (vol_name.value or DEFAULT_CEPHFS_VOLUME or "").strip()
            if not cl or not vol:
                return
            resp = ceph.list_subvolumes(cluster=cl, vol_name=vol, group_name=None, info=False)
            names = _subvols_from_resp(resp)
            nonlocal all_subvols_cache
            all_subvols_cache = names
            all_subvols_sel.options = names; all_subvols_sel.disabled = not bool(names)
            _log(f"Loaded {len(names)} subvolume(s) (no group filter).")
        except Exception as e:
            all_subvols_sel.options = []; all_subvols_sel.disabled = True
            _log(f"Refresh all subvols failed: {e}")

    def _find_group_for_subvol(subvol: str) -> Optional[str]:
        try:
            cl = (cluster_dd.value or "").strip()
            vol = (vol_name.value or "").strip()
            if not cl or not vol or not subvol:
                return None
            resp_g = ceph.list_subvolume_groups(cluster=cl, vol_name=vol, info=False)
            groups = _groups_from_resp(resp_g)
            for g in groups:
                try:
                    resp_s = ceph.list_subvolumes(cluster=cl, vol_name=vol, group_name=g, info=False)
                    names = set(_subvols_from_resp(resp_s))
                    if subvol in names:
                        return g
                except Exception:
                    continue
        except Exception:
            return None
        return None

    # ---------- Connect ----------
    def _on_connect(_):
        nonlocal ceph, reports, core_api

        # Core API (for project membership lookups)
        try:
            if CoreApiClass and (core_api_url.value.strip() or DEFAULT_CORE_API_BASE):
                core_api = CoreApiClass(core_api_host=(core_api_url.value.strip() or DEFAULT_CORE_API_BASE),
                                        token_file=token_file.value.strip())
                _log("Connected to Core API.")
            else:
                core_api = None
                _log(f"Core API import/URL issue: {core_err}")
        except Exception as e:
            core_api = None
            _log(f"Core API connect failed: {e}")

        # Reports
        try:
            if ReportsClient and (reports_url.value.strip() or DEFAULT_REPORTS_API_BASE):
                if not token_file.value.strip():
                    _log("ReportsApi requires token_file JSON with id_token; none provided.")
                reports = ReportsClient(base_url=(reports_url.value.strip() or DEFAULT_REPORTS_API_BASE),
                                        token_file=token_file.value.strip())
                _log("Connected to Reports API.")
            else:
                _log(f"ReportsApi import issue: {reports_err}")
        except Exception as e:
            reports = None
            _log(f"Reports connect failed: {e}")

        # Ceph
        try:
            if CephClient and (ceph_url.value.strip() or DEFAULT_CEPH_API_BASE):
                ceph = CephClient(base_url=(ceph_url.value.strip() or DEFAULT_CEPH_API_BASE),
                                  token_file=(token_file.value.strip() or None),
                                  verify=bool(verify.value))
                status.value = "<b>Connected to Ceph API</b>"
                _populate_clusters()
                _refresh_groups()
                _refresh_all_subvols()
                _refresh_cephx_users()
            else:
                status.value = "<span style='color:red'>Ceph client import/URL missing.</span>"
        except Exception as e:
            ceph = None
            status.value = f"<span style='color:red'>Ceph connect failed: {e}</span>"
            return

        _populate_lists()
        _set_scope()
        _toggle_apply_target()
        _log("Ready.")

    # ---------- Scope / param helpers ----------
    def _set_scope():
        if scope.value == "user":
            user_dd.disabled = False; project_dd.disabled = True
            name.description = "Subvol Name (auto)"
            if user_dd.options:
                _on_user_change(None)
        else:
            user_dd.disabled = True; project_dd.disabled = False
            name.description = "Subvol Name (required)"; name.disabled = False; name.value = ""
            try:
                if (project_dd.value is not None) and project_dd.value < len(projects_cache):
                    name.value = _slugify_subvol_from_project(projects_cache[project_dd.value])
            except Exception:
                pass

    def _on_user_change(_):
        if scope.value == "user" and user_dd.value is not None and user_dd.value < len(users_cache):
            u = users_cache[user_dd.value]
            bl = u.get("bastion_login") or ""
            name.value = bl; name.disabled = True
        else:
            name.disabled = False

    def _on_project_change(_):
        if scope.value == "project":
            try:
                if (project_dd.value is not None) and project_dd.value < len(projects_cache):
                    name.value = _slugify_subvol_from_project(projects_cache[project_dd.value])
            except Exception:
                pass

    def _resolved_create_params() -> Tuple[str, str, str, Optional[str], int]:
        cl = (cluster_dd.value or DEFAULT_CLUSTER or "").strip()
        if not cl:
            raise ValueError("Cluster is required")
        vol = (vol_name.value or DEFAULT_CEPHFS_VOLUME or "").strip()
        if not vol:
            raise ValueError("FS Volume is required")

        if scope.value == "user":
            if user_dd.value is None or user_dd.value >= len(users_cache):
                raise ValueError("Select a user")
            u = users_cache[user_dd.value]
            bl = (u.get("bastion_login") or "").strip()
            if not bl:
                raise ValueError("Selected user has no bastion_login")
            subvol = bl; group = None
        else:
            if project_dd.value is None or project_dd.value >= len(projects_cache):
                raise ValueError("Select a project")
            p = projects_cache[project_dd.value]
            proj_id = (p.get("project_id") or "").strip()
            if not proj_id:
                raise ValueError("Selected project has no project_id")
            group = proj_id
            subvol = (name.value or "").strip()
            if not subvol:
                raise ValueError("Enter a subvolume name")

        sz = _bytes_from_gib(int(size_gib.value or 0))
        return cl, vol, subvol, group, sz

    # ---------- Basic CephFS actions ----------
    def _on_create(_):
        if ceph is None:
            status.value = "<span style='color:red'>Connect first.</span>"; return
        try:
            cl, vol, subvol, group, sz = _resolved_create_params()
            res = ceph.create_or_resize_subvolume(cluster=cl, vol_name=vol, subvol_name=subvol,
                                                  group_name=group, size=sz)
            with results:
                clear_output(); print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Create/Resize OK: {subvol} (group={group})")
            _refresh_subvols(); _refresh_all_subvols()
        except Exception as e:
            status.value = f"<span style='color:red'>Create/Resize failed: {e}</span>"

    def _info_selected(grp: Optional[str], subvol: str):
        cl = (cluster_dd.value or "").strip()
        vol = (vol_name.value or "").strip()
        return ceph.get_subvolume_info(cluster=cl, vol_name=vol, subvol_name=subvol, group_name=grp)

    def _delete_selected(grp: Optional[str], subvol: str):
        cl = (cluster_dd.value or "").strip()
        vol = (vol_name.value or "").strip()
        return ceph.delete_subvolume(cluster=cl, vol_name=vol, subvol_name=subvol, group_name=grp, force=False)

    def _on_info_selected(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        try:
            grp = groups_dd.value; svn = subvols_dd.value
            if not (grp and svn): raise ValueError("Choose group & subvolume.")
            res = _info_selected(grp, svn)
            with results: clear_output(); print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Info OK: {svn} (group={grp})")
        except Exception as e:
            status.value = f"<span style='color:red'>Info (selected) failed: {e}</span>"

    def _on_delete_selected(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        if not confirm_del_chk.value:
            status.value = "<span style='color:red'>Check 'Confirm deletions'.</span>"; return
        try:
            grp = groups_dd.value; svn = subvols_dd.value
            if not (grp and svn): raise ValueError("Choose group & subvolume.")
            res = _delete_selected(grp, svn)
            with results: clear_output(); print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Deleted: {svn} (group={grp})")
            _refresh_subvols(); _refresh_all_subvols()
        except Exception as e:
            status.value = f"<span style='color:red'>Delete (selected) failed: {e}</span>"

    def _on_delete_group(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        if not confirm_del_chk.value:
            status.value = "<span style='color:red'>Check 'Confirm deletions'.</span>"; return
        try:
            grp = groups_dd.value
            if not grp: raise ValueError("Choose group.")
            cl = (cluster_dd.value or "").strip()
            vol = (vol_name.value or "").strip()
            resp = ceph.list_subvolumes(cluster=cl, vol_name=vol, group_name=grp, info=False)
            subnames = _subvols_from_resp(resp)
            ok = err = 0
            with results:
                clear_output(); print(f"Deleting {len(subnames)} subvolume(s) from group '{grp}'...")
            for name_ in subnames:
                try:
                    ceph.delete_subvolume(cluster=cl, vol_name=vol, subvol_name=name_, group_name=grp, force=False)
                    ok += 1; _log(f"Deleted: {name_}")
                except Exception as de:
                    err += 1; _log(f"Failed delete {name_}: {de}")
            with results: print(f"Done. Success: {ok}, Failed: {err}")
            _refresh_subvols(); _refresh_all_subvols()
        except Exception as e:
            status.value = f"<span style='color:red'>Delete Group failed: {e}</span>"

    def _on_remove_group(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        try:
            grp = groups_dd.value
            if not grp: raise ValueError("Choose group.")
            cl = (cluster_dd.value or "").strip()
            vol = (vol_name.value or "").strip()
            if hasattr(ceph, "delete_subvolume_group"):
                res = ceph.delete_subvolume_group(cluster=cl, vol_name=vol, group_name=grp, force=False)
                with results: clear_output(); print(json.dumps(res, indent=2, sort_keys=True))
                _log(f"Removed empty group: {grp}")
            else:
                _log("Client has no delete_subvolume_group(); skipping remove.")
            _refresh_groups()
        except Exception as e:
            status.value = f"<span style='color:red'>Remove Group failed: {e}</span>"

    def _on_info_all(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        try:
            svn = all_subvols_sel.value
            if not svn: raise ValueError("Pick a subvolume in 'All Subvols'.")
            grp = _find_group_for_subvol(svn)  # may be None
            res = _info_selected(grp, svn)
            with results: clear_output(); print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Info (all) OK: {svn} (group={grp})")
        except Exception as e:
            status.value = f"<span style='color:red'>Info (all) failed: {e}</span>"

    def _on_delete_all(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        if not confirm_del_chk.value:
            status.value = "<span style='color:red'>Check 'Confirm deletions'.</span>"; return
        try:
            svn = all_subvols_sel.value
            if not svn: raise ValueError("Pick a subvolume in 'All Subvols'.")
            grp = _find_group_for_subvol(svn)
            res = _delete_selected(grp, svn)
            with results: clear_output(); print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Deleted (all): {svn} (group={grp})")
            _refresh_all_subvols(); _refresh_subvols()
        except Exception as e:
            status.value = f"<span style='color:red'>Delete (all) failed: {e}</span>"

    # ---------- Apply / Revoke caps ----------
    def _update_apply_selection_badge():
        try:
            fs = (vol_name.value or "").strip()
            grp = groups_dd.value
            svn = subvols_dd.value
            if not svn:
                svn = all_subvols_sel.value
                if svn:
                    grp = _find_group_for_subvol(svn)
            if fs and svn:
                grp_disp = grp if grp is not None else "∅ (no group)"
                apply_selection_lbl.value = (
                    f"<b>Selected volume:</b> FS=<code>{fs}</code>, "
                    f"Group=<code>{grp_disp}</code>, Subvol=<code>{svn}</code>"
                )
            else:
                apply_selection_lbl.value = "<i>No volume selected yet — pick from Group/Subvol or All Subvols.</i>"
        except Exception:
            apply_selection_lbl.value = "<i>No volume selected yet — pick from Group/Subvol or All Subvols.</i>"

    def _current_apply_selection() -> Tuple[str, Optional[str], str]:
        fs = (vol_name.value or "").strip()
        if not fs:
            raise ValueError("FS Volume is empty.")
        grp = groups_dd.value
        svn = subvols_dd.value
        if not svn:
            svn = all_subvols_sel.value
            if not svn:
                raise ValueError("Pick a subvolume in Group/Subvol or All Subvols.")
            grp = _find_group_for_subvol(svn)
        return fs, grp, svn

    def _apply_caps_for_users(logins: List[str], fs: str, svn: str, grp: Optional[str], cl: str):
        tmpl_caps = [
            {"entity": "mon", "cap": "allow r fsname={fs}"},
            {"entity": "mds", "cap": "allow rw fsname={fs} path={path}"},
            {"entity": "osd", "cap": "allow rw tag cephfs data={fs}"},
            {"entity": "osd", "cap": "allow rw tag cephfs metadata={fs}"},
        ]
        ok = err = 0
        with results:
            print(f"Applying caps to {len(logins)} user(s) for FS='{fs}', group='{grp}', subvol='{svn}' ...")
        for login in logins:
            user_entity = f"client.{login}"
            try:
                resp = ceph.apply_user_for_multiple_subvols(
                    cluster=cl,
                    user_entity=user_entity,
                    template_capabilities=tmpl_caps,
                    contexts=[(fs, svn, grp)],
                    merge_strategy="multi",
                    dry_run=False,
                )
                ok += 1
                _log(f"OK: {user_entity} → {fs}/{grp or '∅'}:{svn}")
                with results:
                    print(f"OK {user_entity}: {json.dumps(resp, indent=2)[:1000]}")
            except Exception as e:
                err += 1
                _log(f"FAIL: {user_entity} → {e}")
                with results:
                    print(f"FAIL {user_entity}: {e}")
        with results:
            print(f"Apply complete. Success: {ok}, Failed: {err}")

    def _overwrite_caps_for_entity(cl: str, user_entity: str, cap_map: Dict[str, str]):
        items = [{"entity": k, "cap": v} for k, v in cap_map.items() if isinstance(v, str) and v.strip()]
        if hasattr(ceph, "overwrite_user_caps"):
            return ceph.overwrite_user_caps(cluster=cl, user_entity=user_entity, capabilities=items)
        return ceph._request("PUT", "/cluster/user", params={"cluster": cl}, json={"user_entity": user_entity, "capabilities": items})

    def _revoke_caps_for_users(logins: List[str], fs: str, svn: str, maybe_group: Optional[str], cl: str):
        try:
            info = ceph.get_subvolume_info(cluster=cl, vol_name=fs, subvol_name=svn, group_name=maybe_group)
            target_path = _deep_find_path(info)
            if not target_path:
                raise ValueError("Could not resolve subvolume path from API response.")
        except Exception as e:
            raise RuntimeError(f"Failed to resolve path for {fs}/{maybe_group or '∅'}:{svn}: {e}")

        ok = err = 0
        with results:
            print(f"Revoking MDS clause for path '{target_path}' from {len(logins)} user(s)...")
        for login in logins:
            user_entity = f"client.{login}"
            try:
                exp = ceph.export_users(cluster=cl, entities=[user_entity])
                blob = ((exp or {}).get("clusters", {}).get(cl, {}) or {}).get(user_entity, "")
                text = (blob if isinstance(blob, str) else str(blob)).replace("\\n", "\n")
                caps_now = _extract_caps_by_entity_from_keyring_text(text)
                mds_now = caps_now.get("mds", "") or ""

                new_mds, changed = _remove_exact_path_clause_from_mds_caps(mds_now, fs_name=fs, target_path=target_path)
                if not changed:
                    _log(f"No matching MDS clause for {user_entity}; skipping.")
                    with results: print(f"SKIP {user_entity}: no matching clause")
                    continue

                final_caps: Dict[str, str] = {}
                if new_mds.strip():
                    final_caps["mds"] = new_mds
                if (caps_now.get("mon") or "").strip():
                    final_caps["mon"] = caps_now["mon"]
                if (caps_now.get("osd") or "").strip():
                    final_caps["osd"] = caps_now["osd"]
                if (caps_now.get("mgr") or "").strip():
                    final_caps["mgr"] = caps_now["mgr"]

                _overwrite_caps_for_entity(cl, user_entity, final_caps)
                ok += 1
                _log(f"Revoked for {user_entity}")
                with results:
                    print(f"OK {user_entity}: revoked path {target_path}")
            except Exception as e:
                err += 1
                _log(f"FAIL revoke {user_entity}: {e}")
                with results:
                    print(f"FAIL {user_entity}: {e}")
        with results:
            print(f"Revoke complete. Success: {ok}, Failed: {err}")

    # ---- membership via Core API ONLY ----
    def _core_project_member_logins(project_id: str) -> List[str]:
        if core_api is None:
            raise RuntimeError("Core API is not connected; cannot resolve project members.")
        try:
            project = core_api.get_project(project_id=project_id)
            members = project.get("project_members", [])
            people = []
            for m in members:
                uuid = m.get("uuid")
                if uuid:
                    try:
                        people.append(core_api.get_person(uuid))
                    except Exception as e:
                        _log(f"get_person({uuid}) failed: {e}")
            logins = [p.get("bastion_login") for p in people if p and p.get("bastion_login")]
            return logins
        except Exception as e:
            _log(f"Core project membership fetch failed: {e}")
            return []

    def _on_apply_caps(_):
        if ceph is None:
            status.value = "<span style='color:red'>Connect first.</span>"; return
        try:
            cl = (cluster_dd.value or "").strip()
            if not cl:
                raise ValueError("No cluster selected.")
            fs, grp, svn = _current_apply_selection()

            if apply_target.value == "user":
                if apply_user_dd.value is None or apply_user_dd.value >= len(users_cache):
                    raise ValueError("Pick a user.")
                login = users_cache[apply_user_dd.value].get("bastion_login")
                if not login:
                    raise ValueError("Selected user has no bastion_login.")
                _apply_caps_for_users([login], fs, svn, grp, cl)
            else:
                if apply_project_dd.value is None or apply_project_dd.value >= len(projects_cache):
                    raise ValueError("Pick a project.")
                proj = projects_cache[apply_project_dd.value]
                proj_id = proj.get("project_id")
                logins = _core_project_member_logins(proj_id)
                if not logins:
                    raise ValueError("No members with bastion_login found for project.")
                _apply_caps_for_users(logins, fs, svn, grp, cl)

        except Exception as e:
            status.value = f"<span style='color:red'>Apply failed: {e}</span>"
            _log(f"Apply error: {e}")

    def _on_revoke_caps(_):
        if ceph is None:
            status.value = "<span style='color:red'>Connect first.</span>"; return
        try:
            cl = (cluster_dd.value or "").strip()
            if not cl:
                raise ValueError("No cluster selected.")
            fs, grp, svn = _current_apply_selection()

            if apply_target.value == "user":
                if apply_user_dd.value is None or apply_user_dd.value >= len(users_cache):
                    raise ValueError("Pick a user.")
                login = users_cache[apply_user_dd.value].get("bastion_login")
                if not login:
                    raise ValueError("Selected user has no bastion_login.")
                _revoke_caps_for_users([login], fs, svn, grp, cl)
            else:
                if apply_project_dd.value is None or apply_project_dd.value >= len(projects_cache):
                    raise ValueError("Pick a project.")
                proj = projects_cache[apply_project_dd.value]
                proj_id = proj.get("project_id")
                logins = _core_project_member_logins(proj_id)
                if not logins:
                    raise ValueError("No members with bastion_login found for project.")
                _revoke_caps_for_users(logins, fs, svn, grp, cl)

        except Exception as e:
            status.value = f"<span style='color:red'>Revoke failed: {e}</span>"
            _log(f"Revoke error: {e}")

    # ---------- Users tab: list/export/delete ----------
    def _users_from_resp(resp: Any) -> List[Dict[str, Any]]:
        data = (resp or {}).get("data", [])
        return [u for u in (data or []) if isinstance(u, dict) and u.get("user_entity")]

    def _refresh_cephx_users(_=None):
        if ceph is None: return
        try:
            cl = (cluster_dd.value or "").strip()
            if not cl: return
            resp = ceph.list_users(cluster=cl)
            nonlocal cephx_users_cache
            cephx_users_cache = _users_from_resp(resp)
            _apply_user_filter()
            users_sel.disabled = not bool(cephx_users_cache)
            _log(f"Loaded {len(cephx_users_cache)} CephX user(s).")
        except Exception as e:
            users_sel.options = []; users_sel.disabled = True
            _log(f"Refresh users failed: {e}")

    def _apply_user_filter(_=None):
        q = (users_filter.value or "").strip().lower()
        options = []
        for i, u in enumerate(cephx_users_cache):
            ent = u.get("user_entity", "")
            if (not q) or (q in ent.lower()):
                options.append((ent, i))
        users_sel.options = options

    def _on_export_users(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        try:
            cl = (cluster_dd.value or "").strip()
            idxs = list(users_sel.value)
            if not idxs: raise ValueError("Select one or more users.")
            entities = [cephx_users_cache[i]["user_entity"] for i in idxs]
            resp = ceph.export_users(cluster=cl, entities=entities)
            clusters = (resp or {}).get("clusters", {})
            with results:
                clear_output()
                print("Exported keyrings:\n")
                for ent in entities:
                    blob = ((clusters.get(cl, {}) or {}).get(ent) or "")
                    text = (blob if isinstance(blob, str) else str(blob)).replace("\\n", "\n")
                    print(f"----- {ent} -----\n{text}\n")
            _log(f"Exported {len(entities)} user(s).")
        except Exception as e:
            status.value = f"<span style='color:red'>Export failed: {e}</span>"

    def _on_delete_users(_):
        if ceph is None: status.value = "<span style='color:red'>Connect first.</span>"; return
        if not confirm_del_usr_chk.value:
            status.value = "<span style='color:red'>Check 'Confirm user deletions'.</span>"; return
        try:
            cl = (cluster_dd.value or "").strip()
            idxs = list(users_sel.value)
            if not idxs: raise ValueError("Select one or more users.")
            entities = [cephx_users_cache[i]["user_entity"] for i in idxs]
            ok = err = 0
            for ent in entities:
                try:
                    ceph.delete_user(cluster=cl, entity=ent); ok += 1; _log(f"Deleted user: {ent}")
                except Exception as de:
                    err += 1; _log(f"Failed delete {ent}: {de}")
            with results:
                clear_output(); print(f"User delete complete. Success: {ok}, Failed: {err}")
            _refresh_cephx_users()
        except Exception as e:
            status.value = f"<span style='color:red'>Delete users failed: {e}</span>"

    # ---------- Wire events ----------
    connect.on_click(_on_connect)

    # Create/resize
    scope.observe(lambda ch: _set_scope(), names="value")
    user_dd.observe(_on_user_change, names="value")
    project_dd.observe(_on_project_change, names="value")
    create_btn.on_click(_on_create)

    # Group browse
    refresh_groups_btn.on_click(_refresh_groups)
    groups_dd.observe(lambda ch: _refresh_subvols(), names="value")
    subvols_dd.observe(lambda ch: _refresh_subvols(), names="value")
    refresh_subvols_btn.on_click(_refresh_subvols)
    info_sel_btn.on_click(_on_info_selected)
    delete_sel_btn.on_click(_on_delete_selected)
    delete_group_btn.on_click(_on_delete_group)
    remove_group_btn.on_click(_on_remove_group)

    # All subvols
    refresh_all_btn.on_click(_refresh_all_subvols)
    info_all_btn.on_click(_on_info_all)
    delete_all_btn.on_click(_on_delete_all)
    all_subvols_sel.observe(lambda ch: _update_apply_selection_badge(), names="value")

    # Apply/Revoke caps
    def _toggle_apply_target(_=None):
        if apply_target.value == "user":
            apply_user_dd.disabled = False; apply_project_dd.disabled = True
        else:
            apply_user_dd.disabled = True; apply_project_dd.disabled = False
    apply_target.observe(_toggle_apply_target, names="value")
    apply_btn.on_click(_on_apply_caps)
    revoke_btn.on_click(_on_revoke_caps)
    _toggle_apply_target()

    # Users tab
    refresh_users_btn.on_click(_refresh_cephx_users)
    users_filter.observe(_apply_user_filter, names="value")
    export_users_btn.on_click(_on_export_users)
    delete_users_btn.on_click(_on_delete_users)

    # ---------- Layout (Tabs) ----------
    hdr = W.VBox([
        W.HTML("<h3>FABRIC CephFS Manager</h3>"),
        W.HBox([ceph_url, reports_url]),
        W.HBox([core_api_url]),
        W.HBox([token_file, verify, cluster_dd, vol_name, connect]),
        W.HTML("<hr>")
    ])

    create_box = W.VBox([
        W.HTML("<b>Create / Resize</b>"),
        W.HBox([scope, project_dd, user_dd]),
        W.HBox([name, size_gib, create_btn]),
    ])

    browse_box = W.VBox([
        W.HTML("<b>Browse by Group</b>"),
        W.HBox([groups_dd, refresh_groups_btn]),
        W.HBox([subvols_dd, refresh_subvols_btn]),
        W.HBox([info_sel_btn, delete_sel_btn, delete_group_btn, remove_group_btn, confirm_del_chk]),
        W.HTML("<b>All Subvolumes (no group filter)</b>"),
        W.HBox([all_subvols_sel, W.VBox([refresh_all_btn, info_all_btn, delete_all_btn])]),
    ])

    apply_box = W.VBox([
        apply_header,
        apply_selection_lbl,
        W.HBox([apply_target, apply_user_dd, apply_project_dd]),
        W.HBox([apply_btn, revoke_btn]),
    ])

    tab_subvols = W.VBox([create_box, W.HTML("<hr>"), browse_box, W.HTML("<hr>"), apply_box])

    users_box = W.VBox([
        W.HTML("<b>CephX Users</b>"),
        W.HBox([users_filter, refresh_users_btn]),
        W.HBox([users_sel, W.VBox([export_users_btn, delete_users_btn, confirm_del_usr_chk])]),
    ])

    tabs = W.Tab(children=[tab_subvols, users_box])
    tabs.set_title(0, "Subvolumes & Groups")
    tabs.set_title(1, "Users")

    ui = W.VBox([
        hdr,
        tabs,
        W.HTML("<hr><b>Results</b>"),
        results,
        W.HTML("<hr><b>Log</b>"),
        log
    ])

    display(ui)
