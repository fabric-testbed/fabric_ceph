# ceph_subvol_gui.py
"""
CephFS Subvolume Manager GUI (FABRIC)

Highlights
----------
- Uses token_file exclusively (no direct token field).
- Default host/url/token settings are overridable via module vars or env.
- Per-User: subvol_name = user's bastion_login (no group)
- Per-Project: group_name = project_id; subvol free text (slug suggested from project name)
- Populates Users/Projects from Reports API
- Ceph ops: create/resize, info, delete subvolume
- Apply Caps (merge) using /cluster/user with multi-context renders
  * Per-Project bulk: for each member → apply to BOTH:
      - per-user subvol (bastion_login, no group)
      - selected project subvol (group=project_id)
  * Per-User single: applies to the per-user subvol
- Caps are sent as placeholders (entity/cap) and the client requests merge_strategy="multi"

Override defaults before calling launch(), or set environment variables:
-----------------------------------------------------------------------
import ceph_subvol_gui as gui
gui.DEFAULT_CEPH_API_BASE = "https://mgr.example/api"
gui.DEFAULT_REPORTS_API_BASE = "https://reports.example/api"
gui.DEFAULT_VERIFY_TLS = True
gui.DEFAULT_CEPHFS_VOLUME = "cephfs"
gui.DEFAULT_CLUSTER = "asia"
gui.DEFAULT_TOKEN_FILE = "/path/to/token.json"

from ceph_subvol_gui import launch
launch()
"""

from __future__ import annotations

import os
import json
import traceback
from typing import Optional, List, Dict, Any, Tuple

# -------- Defaults (override in notebook or via env) --------
DEFAULT_CEPH_API_BASE: Optional[str] = os.getenv("FABRIC_CEPH_API_BASE")
DEFAULT_REPORTS_API_BASE: Optional[str] = os.getenv("FABRIC_REPORTS_API_BASE")
DEFAULT_VERIFY_TLS: bool = os.getenv("FABRIC_VERIFY_TLS", "true").lower() in {"1", "true", "yes", "on"}
DEFAULT_CEPHFS_VOLUME: str = os.getenv("FABRIC_CEPHFS_VOLUME", "cephfs")
DEFAULT_CLUSTER: Optional[str] = os.getenv("FABRIC_CEPH_CLUSTER")
DEFAULT_TOKEN_FILE: Optional[str] = os.getenv("FABRIC_TOKEN_FILE")

# -------- Dynamic imports --------
def _import_ceph_client():
    try:
        from fabric_ceph_client import CephManagerClient  # local fallback path
        return CephManagerClient, None
    except Exception as e1:
        try:
            from fabric_ceph_client.fabric_ceph_client import CephManagerClient  # packaged module
            return CephManagerClient, None
        except Exception as e2:
            return None, f"{e1!r}; {e2!r}"

def _import_reports_client():
    try:
        from reports_api import ReportsApi  # local fallback path
        return ReportsApi, None
    except Exception as e1:
        try:
            from fabric_reports_client.reports_api import ReportsApi  # packaged module
            return ReportsApi, None
        except Exception as e2:
            return None, f"{e1!r}; {e2!r}"

# -------- Helpers --------

def _slugify_subvol_from_project(p: dict) -> str:
    """Create a safe default subvol name from project metadata."""
    import re
    base = (p.get("project_name") or p.get("project_id") or "project").lower()
    base = re.sub(r"[^a-z0-9\-]+", "-", base).strip("-")
    base = re.sub(r"-{2,}", "-", base)
    if not base:
        base = (p.get("project_id") or "project")[:12].lower()
    return base[:63]  # keep it reasonable

def _unescape_keyring_blob(blob: Any) -> str:
    # blob may be base64 or string with escaped newlines; handle simple escaped newlines
    if not isinstance(blob, str):
        try:
            blob = blob.decode("utf-8", "ignore")
        except Exception:
            blob = str(blob)
    # Replace common JSON-escaped newlines
    return blob.replace("\\n", "\n")

def extract_caps_by_entity(keyring_text: str) -> Dict[str, str]:
    """
    Parse lines like:
      caps mds = "allow rw fsname=cephfs path=/foo, allow r fsname=cephfs path=/bar"
      caps mon = "allow r fsname=cephfs"
      caps osd = "allow rw tag cephfs data=cephfs, allow rw tag cephfs metadata=cephfs"
    Return dict: {"mds": "...", "mon": "...", "osd": "..."}
    """
    import re
    caps: Dict[str, str] = {}
    for ent in ("mds", "mon", "osd"):
        m = re.search(rf'caps\s+{ent}\s*=\s*"([^"]+)"', keyring_text)
        if m:
            caps[ent] = m.group(1)
    return caps

def _bytes_from_gib(gib: int) -> int:
    return int(gib) * 1024**3

def _extract_users(rows: Any) -> List[Dict[str, Any]]:
    """
    Expect rows like [{"bastion_login": "...", "user_email": "...", "user_id": "...", ...}, ...]
    We preserve full dict per item so we can reference other fields if needed.
    """
    if isinstance(rows, dict):
        rows = rows.get("data", [])
    out = []
    for r in rows or []:
        if isinstance(r, dict) and ("bastion_login" in r or "user_email" in r or "user_id" in r):
            out.append(r)
    # de-dup by bastion_login or user_email
    seen = set()
    uniq = []
    for r in out:
        key = r.get("bastion_login") or r.get("user_email") or r.get("user_id")
        if key and key not in seen:
            seen.add(key)
            uniq.append(r)
    return uniq

def _extract_projects(rows: Any) -> List[Dict[str, Any]]:
    """
    Expect rows like [{"project_id": "...", "project_name": "...", ...}, ...]
    """
    if isinstance(rows, dict):
        rows = rows.get("data", [])
    out = []
    for r in rows or []:
        if isinstance(r, dict) and "project_id" in r:
            out.append(r)
    # de-dup by project_id
    seen = set()
    uniq = []
    for r in out:
        key = r.get("project_id")
        if key and key not in seen:
            seen.add(key)
            uniq.append(r)
    return uniq

def _to_context_tuples(contexts: List[Any]) -> List[Tuple[str, str, Optional[str]]]:
    """
    Accepts list of dicts ({fs_name, subvol_name, [group_name]}) or tuples,
    returns list of (fs_name, subvol_name, group_name|None) tuples with de-dupe.
    """
    out: List[Tuple[str, str, Optional[str]]] = []
    seen = set()
    for c in contexts:
        if isinstance(c, dict):
            fsn = c["fs_name"]; svn = c["subvol_name"]; grp = c.get("group_name")
        else:
            fsn, svn, grp = c
        key = (fsn, svn, grp or None)
        if key not in seen:
            seen.add(key)
            out.append((fsn, svn, grp))
    return out

# -------- GUI --------

def launch():
    import ipywidgets as W
    from IPython.display import display, clear_output

    CephClient, ceph_err = _import_ceph_client()
    ReportsClient, reports_err = _import_reports_client()

    # Connection inputs
    ceph_url = W.Text(description="Ceph API", value=(DEFAULT_CEPH_API_BASE or ""), placeholder="https://mgr/api", layout=W.Layout(width="55%"))
    reports_url = W.Text(description="Reports API", value=(DEFAULT_REPORTS_API_BASE or ""), placeholder="https://reports/api", layout=W.Layout(width="55%"))
    token_file = W.Text(description="Token File", value=(DEFAULT_TOKEN_FILE or ""), placeholder="Path to JSON with id_token")
    verify = W.Checkbox(value=DEFAULT_VERIFY_TLS, description="Verify TLS")
    cluster = W.Text(description="Cluster", value=(DEFAULT_CLUSTER or ""), placeholder="e.g., asia")
    vol_name = W.Text(description="FS Volume", value=DEFAULT_CEPHFS_VOLUME, placeholder="CephFS vol_name (e.g., cephfs)")
    connect = W.Button(description="Connect", button_style="primary")

    # Scope & pickers
    scope = W.ToggleButtons(options=[("Per-User", "user"), ("Per-Project", "project")], description="Scope")
    project_dd = W.Dropdown(description="Project", options=[], disabled=True)
    user_dd = W.Dropdown(description="User", options=[], disabled=False)
    refresh_lists = W.Button(description="Refresh Lists")

    # Subvolume inputs
    name = W.Text(description="Subvol Name", placeholder="Per-user: auto (bastion). Per-project: required.")
    size_gib = W.BoundedIntText(description="Size (GiB)", min=1, max=1024 * 1024, value=50)
    create_btn = W.Button(description="Create/Resize", button_style="success")
    info_btn = W.Button(description="Get Info")
    delete_btn = W.Button(description="Delete", button_style="danger")

    # ---- User / Caps controls ----
    WUC = W.HTML("<b>User Caps</b>")
    user_entity_fmt = W.Text(description="Entity fmt", value="client.{bastion_login}", layout=W.Layout(width="50%"))
    apply_user_caps_btn = W.Button(description="Apply Caps (single/bulk)", button_style="warning")

    # Output areas
    status = W.HTML(value="")
    results = W.Output(layout={"border": "1px solid #eee", "min_height": "160px"})
    log = W.Output(layout={"border": "1px solid #ddd"})

    # State
    ceph = None
    reports = None
    users_cache: List[Dict[str, Any]] = []
    projects_cache: List[Dict[str, Any]] = []

    def _log(msg: str):
        from datetime import datetime
        with log:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def _set_scope():
        if scope.value == "user":
            user_dd.disabled = False
            project_dd.disabled = True
            if user_dd.options:
                _on_user_change(None)
            name.description = "Subvol Name (auto)"
        else:
            user_dd.disabled = True
            project_dd.disabled = False
            name.disabled = False
            name.description = "Subvol Name (required)"
            name.value = ""
            try:
                if (not name.value) and (project_dd.value is not None) and project_dd.value < len(projects_cache):
                    pmeta = projects_cache[project_dd.value]
                    name.value = _slugify_subvol_from_project(pmeta)
            except Exception:
                pass
            project_dd.disabled = False
            name.disabled = False

    def _format_user_label(u: Dict[str, Any]) -> str:
        bl = u.get("bastion_login") or ""
        em = u.get("user_email") or ""
        return f"{bl or em} | {em}" if bl and em else (bl or em or u.get("user_id", "<unknown>"))

    def _format_project_label(p: Dict[str, Any]) -> str:
        return f"{p.get('project_name') or p.get('project_id')} [{p.get('project_id')}]"

    def _populate_lists(_=None):
        nonlocal users_cache, projects_cache
        try:
            if reports:
                users = reports.query_users(user_active=True, fetch_all=True)
                projects = reports.query_projects(project_active=True, fetch_all=True)
                users_cache = _extract_users(users)
                projects_cache = _extract_projects(projects)
                user_dd.options = [(_format_user_label(u), i) for i, u in enumerate(users_cache)]
                project_dd.options = [(_format_project_label(p), i) for i, p in enumerate(projects_cache)]
                _log(f"Loaded {len(users_cache)} users, {len(projects_cache)} projects from Reports.")
            else:
                user_dd.options = []
                project_dd.options = []
                users_cache = []
                projects_cache = []
                _log("Reports API not connected; pickers are empty.")
        except Exception as e:
            _log(f"Failed to populate lists: {e}")

    def _on_connect(_):
        nonlocal ceph, reports
        # Reports
        try:
            if ReportsClient and (reports_url.value.strip() or DEFAULT_REPORTS_API_BASE):
                if not token_file.value.strip():
                    _log("ReportsApi requires token_file JSON with id_token; none provided.")
                reports = ReportsClient(
                    base_url=(reports_url.value.strip() or DEFAULT_REPORTS_API_BASE),
                    token_file=token_file.value.strip(),
                )
                _log("Connected to Reports API.")
            else:
                _log(f"ReportsApi import issue: {reports_err}")
        except Exception as e:
            reports = None
            _log(f"Reports connect failed: {e}")

        # Ceph
        try:
            if CephClient and (ceph_url.value.strip() or DEFAULT_CEPH_API_BASE):
                ceph = CephClient(
                    base_url=(ceph_url.value.strip() or DEFAULT_CEPH_API_BASE),
                    token_file=(token_file.value.strip() or None),
                    verify=bool(verify.value),
                )
                status.value = "<b>Connected to Ceph API</b>"
            else:
                status.value = "<span style='color:red'>Ceph client import/URL missing.</span>"
        except Exception as e:
            ceph = None
            status.value = f"<span style='color:red'>Ceph connect failed: {e}</span>"
            return

        _populate_lists()
        _set_scope()
        _log("Ready.")

    def _resolved_params() -> Tuple[str, str, str, Optional[str], int]:
        cl = (cluster.value or DEFAULT_CLUSTER or "").strip()
        if not cl:
            raise ValueError("Cluster is required")
        vol = (vol_name.value or DEFAULT_CEPHFS_VOLUME or "").strip()
        if not vol:
            raise ValueError("FS Volume (vol_name) is required")

        if scope.value == "user":
            if user_dd.value is None or user_dd.value >= len(users_cache):
                raise ValueError("Select a user")
            u = users_cache[user_dd.value]
            bl = (u.get("bastion_login") or "").strip()
            if not bl:
                raise ValueError("Selected user has no bastion_login")
            subvol = bl
            group = None
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

    def _on_user_change(_):
        if scope.value == "user" and user_dd.value is not None and user_dd.value < len(users_cache):
            u = users_cache[user_dd.value]
            bl = u.get("bastion_login") or ""
            name.value = bl
            name.disabled = True
        else:
            name.disabled = False

    def _on_project_change(_):
        if scope.value == "project":
            try:
                if (not name.value) and (project_dd.value is not None) and project_dd.value < len(projects_cache):
                    pmeta = projects_cache[project_dd.value]
                    name.value = _slugify_subvol_from_project(pmeta)
            except Exception:
                pass

    # ---- Basic CephFS actions ----

    def _on_create(_):
        if ceph is None:
            status.value = "<span style='color:red'>Connect to Ceph API first.</span>"
            return
        try:
            cl, vol, subvol, group, sz = _resolved_params()
            res = ceph.create_or_resize_subvolume(cluster=cl, vol_name=vol, subvol_name=subvol, group_name=group, size=sz)
            with results:
                clear_output()
                print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Create/Resize OK: {subvol} (group={group})")
        except Exception as e:
            status.value = f"<span style='color:red'>Create/Resize failed: {e}</span>"

    def _on_info(_):
        if ceph is None:
            status.value = "<span style='color:red'>Connect to Ceph API first.</span>"
            return
        try:
            cl, vol, subvol, group, _ = _resolved_params()
            res = ceph.get_subvolume_info(cluster=cl, vol_name=vol, subvol_name=subvol, group_name=group)
            with results:
                clear_output()
                print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Info OK: {subvol} (group={group})")
        except Exception as e:
            status.value = f"<span style='color:red'>Info failed: {e}</span>"

    def _on_delete(_):
        if ceph is None:
            status.value = "<span style='color:red'>Connect to Ceph API first.</span>"
            return
        try:
            cl, vol, subvol, group, _ = _resolved_params()
            res = ceph.delete_subvolume(cluster=cl, vol_name=vol, subvol_name=subvol, group_name=group, force=False)
            with results:
                clear_output()
                print(json.dumps(res, indent=2, sort_keys=True))
            _log(f"Delete OK: {subvol} (group={group})")
        except Exception as e:
            status.value = f"<span style='color:red'>Delete failed: {e}</span>"

    # ---- Caps helpers ----

    def _user_entity_for(login: str) -> str:
        fmt = (user_entity_fmt.value or "client.{bastion_login}")
        return fmt.replace("{bastion_login}", login)

    def _project_members(proj_id: str) -> List[Dict[str, Any]]:
        """Return list of user dicts (must include bastion_login) for a project."""
        members: List[Dict[str, Any]] = []
        try:
            # ReportsApi expects a list for project_id filter
            resp = reports.query_users(user_active=True, fetch_all=True, project_id=[proj_id])
            data = (resp or {}).get("data", [])
        except Exception as e:
            _log(f"query_users(project_id=...) failed: {e}")
            data = []

        if not data:
            return members

        # Map to cached users to pick up bastion_login (or use row directly)
        for m in data:
            uid = m.get("user_id")
            uem = m.get("user_email")
            u = next((c for c in users_cache if (uid and c.get("user_id") == uid) or (uem and c.get("user_email") == uem)), None)
            if not u:
                u = m
            if u and u.get("bastion_login"):
                members.append(u)
        return members

    def _contexts_current(fs_name: str, subvol_name: str, group_name: Optional[str]):
        ctx = {"fs_name": fs_name, "subvol_name": subvol_name}
        if group_name:
            ctx["group_name"] = group_name
        return ctx

    def _apply_caps_multicontext(login: str, contexts: List[Dict[str, Any]] | List[Tuple[str, str, Optional[str]]], cl: str):
        """
        Export current user caps to avoid redundant writes, then request multi-context apply.
        We ONLY send placeholder caps (entity/cap). Backend will render {fs}/{path} per context and merge.
        """
        user_entity = _user_entity_for(login)

        # 1) Fetch current MDS caps to skip already covered subvols
        try:
            resp = ceph.export_users(cluster=cl, entities=[user_entity])
            blob = (resp or {}).get("clusters", {}).get(cl, {}).get(user_entity)
            mds_now = ""
            if blob:
                text = _unescape_keyring_blob(blob)
                caps_now = extract_caps_by_entity(text)  # {"mds": "...", "mon": "...", "osd": "..."}
                mds_now = caps_now.get("mds", "") or ""
        except Exception:
            mds_now = ""

        # 2) Only push contexts whose subvol name isn’t already present in MDS caps
        needed: List[Tuple[str, str, Optional[str]]] = []
        for c in contexts:
            if isinstance(c, dict):
                fsn, svn, grp = c["fs_name"], c["subvol_name"], c.get("group_name")
            else:
                fsn, svn, grp = c
            if svn and (svn not in mds_now):
                needed.append((fsn, svn, grp))
        needed = _to_context_tuples(needed)
        if not needed:
            return {"note": "No changes needed; all contexts already in MDS caps.",
                    "entity": user_entity, "contexts": contexts}

        # 3) Placeholder-only template (entity/cap!) — server will render {fs}/{path} and merge
        tmpl_caps = [
            {"entity": "mon", "cap": "allow r fsname={fs}"},
            {"entity": "mds", "cap": "allow rw fsname={fs} path={path}"},
            {"entity": "osd", "cap": "allow rw tag cephfs data={fs}"},
            {"entity": "osd", "cap": "allow rw tag cephfs metadata={fs}"},
        ]

        return ceph.apply_user_for_multiple_subvols(
            cluster=cl,
            user_entity=user_entity,
            template_capabilities=tmpl_caps,
            contexts=needed,            # list[(fs, subvol, group|None)]
            merge_strategy="multi",     # request server merge (append, no overwrite)
            dry_run=False
        )

    # ---- Caps button handler ----

    def _on_apply_user_caps(_):
        if ceph is None:
            status.value = "<span style='color:red'>Connect to Ceph API first.</span>"
            return
        try:
            cl, vol, subvol, group, _ = _resolved_params()
            if scope.value == "user":
                # Selected single user → apply to their per-user subvol
                u = users_cache[user_dd.value]
                bl = u.get("bastion_login")
                contexts = [{"fs_name": vol, "subvol_name": bl}]  # no group key when None
                res = _apply_caps_multicontext(bl, contexts, cl)
                with results:
                    clear_output()
                    print(f"Applied caps to: {bl}")
                    print("Contexts:", json.dumps(contexts, indent=2))
                    print(json.dumps(res, indent=2, sort_keys=True))
                _log(f"Caps applied to {bl} across {len(contexts)} context(s).")
            else:
                # Per-Project bulk: for each member include BOTH contexts
                p = projects_cache[project_dd.value]
                proj_id = p.get("project_id")
                members = _project_members(proj_id)
                with results:
                    clear_output()
                    print(f"Applying caps to {len(members)} members...")
                ok = 0
                errs = 0
                for u in members:
                    bl = u.get("bastion_login")
                    contexts = [
                        _contexts_current(vol, subvol, group),  # project
                    ]
                    try:
                        _apply_caps_multicontext(bl, contexts, cl)
                        ok += 1
                    except Exception as e:
                        errs += 1
                        _log(f"Failed caps for {bl}: {e}")
                with results:
                    print(f"Done. Success: {ok}, Failed: {errs}")
                _log(f"Bulk caps done across both contexts: ok={ok}, err={errs}")
        except Exception as e:
            status.value = f"<span style='color:red'>Apply caps failed: {e} {traceback.format_exc()}</span>"

    # ---- Wire events & Layout ----

    connect.on_click(_on_connect)
    refresh_lists.on_click(_populate_lists)
    scope.observe(lambda ch: _set_scope(), names="value")
    user_dd.observe(_on_user_change, names="value")
    project_dd.observe(_on_project_change, names="value")
    create_btn.on_click(_on_create)
    info_btn.on_click(_on_info)
    delete_btn.on_click(_on_delete)
    apply_user_caps_btn.on_click(_on_apply_user_caps)

    # Layout
    line1 = W.HBox([ceph_url, reports_url])
    line2 = W.HBox([token_file, verify, cluster, vol_name, connect, refresh_lists])
    line3 = W.HBox([scope, project_dd, user_dd])
    line4 = W.HBox([name, size_gib])
    line5 = W.HBox([create_btn, info_btn, delete_btn])
    line_caps1 = W.HBox([user_entity_fmt])
    line_caps4 = W.HBox([apply_user_caps_btn])

    ui = W.VBox([
        W.HTML("<h3>FABRIC CephFS Subvolume Manager</h3>"),
        status,
        line1, line2,
        W.HTML("<hr><b>Scope & Pickers</b>"),
        line3,
        W.HTML("<b>Subvolume</b>"),
        line4, line5,
        W.HTML("<hr><b>User Creation / Caps</b>"),
        WUC, line_caps1, line_caps4,
        W.HTML("<hr><b>Results</b>"),
        results,
        W.HTML("<hr><b>Log</b>"),
        log
    ])
    display(ui)
