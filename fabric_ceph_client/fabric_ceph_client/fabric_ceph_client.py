# fabric_ceph_client.py
"""
Ceph Manager Client (Python)
----------------------------

Minimal, friendly wrapper for the FABRIC Ceph Manager service.

Per-cluster API change:
- All CephFS and Cluster User calls now take `cluster: str` and send it as a
  query parameter (?cluster=<name>) to routes like `/cluster/user`,
  `/cephfs/subvolume/{vol_name}`, etc. No X-Cluster header, no /{cluster}/ prefix.

Auth:
- Token via `token` or `token_file` (JSON: reads `id_token` by default).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, Tuple
from pathlib import Path
import json
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --------------------- Exceptions ---------------------

class ApiError(RuntimeError):
    def __init__(self, status: int, url: str, message: str = "", payload: Any = None):
        super().__init__(f"[{status}] {url} :: {message or payload}")
        self.status = status
        self.url = url
        self.message = message
        self.payload = payload


# --------------------- Client ---------------------

@dataclass
class CephManagerClient:
    base_url: str

    # Auth options (choose one):
    token: Optional[str] = None
    token_file: Optional[Union[str, Path]] = None
    token_key: str = "id_token"  # JSON field to extract when reading token_file

    timeout: int = 60
    verify: bool = True
    accept: str = "application/json, text/plain"

    # Deprecated (kept only to avoid breaking ctor signatures)
    default_x_cluster: Optional[str] = None  # DEPRECATED

    # internal
    _session: requests.Session = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")

        # env fallbacks
        if not self.token and not self.token_file:
            env_token = os.getenv("FABRIC_CEPH_TOKEN")
            env_token_file = os.getenv("FABRIC_CEPH_TOKEN_FILE")
            if env_token:
                self.token = env_token
            elif env_token_file:
                self.token_file = env_token_file

        if self.token_file and not self.token:
            self.refresh_token_from_file()

        self._session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "PUT", "DELETE", "POST"}),
            raise_on_status=False,
        )
        self._session.mount("http://", HTTPAdapter(max_retries=retry))
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    # ----- auth helpers -----

    def refresh_token_from_file(self) -> None:
        if not self.token_file:
            raise ValueError("token_file is not set")

        path = Path(self.token_file).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Token file not found: {path}")

        raw = path.read_text(encoding="utf-8").strip()
        try:
            obj = json.loads(raw)
            token = (
                obj.get(self.token_key)
                or obj.get("access_token")
                or obj.get("token")
            )
            if not token:
                raise KeyError(
                    f"Token file JSON does not contain '{self.token_key}', 'access_token', or 'token'."
                )
            self.token = str(token).strip()
        except json.JSONDecodeError:
            self.token = raw

        if not self.token:
            raise ValueError("Token could not be loaded from token_file")

    # ----- internals -----

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {"Accept": self.accept}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if extra:
            h.update(extra)
        return h

    @staticmethod
    def _is_json(resp: requests.Response) -> bool:
        ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        return ct.endswith("/json") or ct.endswith("+json")

    # --- add near other static helpers ---
    @staticmethod
    def _norm_cap_kv(it: Dict[str, str]) -> Dict[str, str]:
        """
        Normalize a single capability to {type, value}.
        Accepts either {"type": "...", "value": "..."} or {"entity": "...", "cap"/"caps": "..."}.
        """
        t = it.get("type") or it.get("entity")
        v = it.get("value") or it.get("cap") or it.get("caps")
        if not t or v is None:
            raise ValueError("Each capability must include 'type'/'entity' and 'value'/'cap'")
        return {"type": str(t), "value": str(v)}

    @staticmethod
    def _normalize_kv_caps(items: Union[List[Dict[str, str]], Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Normalize either:
          - list[ {type/value} or {entity/cap} ]
          - dict { component -> rule }
        Into list[{type, value}].
        """
        if isinstance(items, dict):
            return [{"type": str(k), "value": str(v)} for k, v in items.items()]
        if isinstance(items, list):
            return [CephManagerClient._norm_cap_kv(i) for i in items]
        raise ValueError("'capabilities' must be a list of objects or a dict of component->rule")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        data: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"

        def _do():
            return self._session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json,
                data=data,
                headers=self._headers(headers),
                timeout=self.timeout,
                verify=self.verify,
            )

        resp = _do()

        # Optional: refresh token on 401 once if token_file is present
        if resp.status_code == 401 and self.token_file:
            try:
                self.refresh_token_from_file()
                resp = _do()
            except Exception:
                pass

        if resp.status_code >= 400:
            payload: Any
            message = ""
            if self._is_json(resp):
                try:
                    payload = resp.json()
                    message = (
                        payload.get("message")
                        or (payload.get("errors", [{}])[0].get("message")
                            if isinstance(payload.get("errors"), list) and payload["errors"] else "")
                        or payload.get("detail")
                        or ""
                    )
                except Exception:
                    payload = resp.text
            else:
                payload = resp.text
            raise ApiError(resp.status_code, url, message=message, payload=payload)

        return resp.json() if self._is_json(resp) else resp.text

    @staticmethod
    def _params_with_cluster(cluster: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not cluster or not cluster.strip():
            raise ValueError("cluster must be a non-empty string")
        out = {"cluster": cluster}
        if extra:
            # don't allow extra to overwrite 'cluster'
            for k, v in extra.items():
                if k != "cluster":
                    out[k] = v
        return out

    # --------------------- Cluster info (global) ---------------------

    def list_cluster_info(self) -> Dict[str, Any]:
        """GET /cluster/info (no per-cluster parameter)."""
        return self._request("GET", "/cluster/info")

    def cluster_minimal_confs(self) -> Dict[str, str]:
        info = self.list_cluster_info()
        out: Dict[str, str] = {}
        items = (info or {}).get("data", []) if isinstance(info, dict) else []
        for item in items:
            if isinstance(item, dict) and not item.get("error"):
                cluster = item.get("cluster")
                conf = item.get("ceph_conf_minimal")
                if cluster and isinstance(conf, str) and conf.strip():
                    out[cluster] = conf
        return out

    @staticmethod
    def _norm_cap_item(it: Dict[str, str]) -> Dict[str, str]:
        t = it.get("entity") or it.get("type")
        c = it.get("cap") or it.get("caps")
        if not t or not c:
            raise ValueError("Each template_capability must include entity/type and cap/caps")
        return {"entity": t, "cap": c}

    @staticmethod
    def _normalize_caps(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
        return [CephManagerClient._norm_cap_item(i) for i in items]

    # --------------------- Cluster User (per cluster via query param) ---------------------

    def apply_user_templated(
        self,
        cluster: str,
        *,
        user_entity: str,
        template_capabilities: List[Dict[str, str]],
        renders: Optional[List[Dict[str, str]]] = None,
        fs_name: Optional[str] = None,
        subvol_name: Optional[str] = None,
        group_name: Optional[str] = None,
        extra_subs: Optional[Dict[str, str]] = None,
        merge_strategy: Optional[str] = None,  # "comma" | "multi" | "auto"
        dry_run: bool = False,
        sync_across_clusters: bool = True,
        preferred_source: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        POST /cluster/user?cluster=<name>

        Always sends `renders` (array). If only a single context is provided,
        normalize it to one item.
        """
        # Normalize contexts -> renders[]
        if renders and len(renders) > 0:
            renders_norm: List[Dict[str, str]] = []
            for rc in renders:
                item = {"fs_name": rc["fs_name"], "subvol_name": rc["subvol_name"]}
                if rc.get("group_name"):
                    item["group_name"] = rc["group_name"]
                renders_norm.append(item)
        else:
            if not fs_name or not subvol_name:
                raise ValueError("Provide either 'renders' (list) or fs_name+subvol_name")
            item = {"fs_name": fs_name, "subvol_name": subvol_name}
            if group_name:
                item["group_name"] = group_name
            renders_norm = [item]

        payload: Dict[str, Any] = {
            "user_entity": user_entity,
            "template_capabilities": template_capabilities,
            "renders": renders_norm,
            "sync_across_clusters": bool(sync_across_clusters),
            "dry_run": bool(dry_run),
        }
        if preferred_source:
            payload["preferred_source"] = preferred_source
        if extra_subs:
            payload["extra_subs"] = extra_subs
        if merge_strategy:
            payload["merge_strategy"] = merge_strategy

        return self._request(
            "POST",
            "/cluster/user",
            params=self._params_with_cluster(cluster),
            json=payload,
        )

    def apply_user_for_multiple_subvols(
        self,
        cluster: str,
        *,
        user_entity: str,
        template_capabilities: List[Dict[str, str]],
        contexts: List[Tuple[str, str, Optional[str]]],  # (fs_name, subvol_name, group_name)
        sync_across_clusters: bool = True,
        preferred_source: Optional[str] = None,
        merge_strategy: Optional[str] = "multi",
        dry_run: bool = False,
        extra_subs: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        renders = []
        for fsn, svn, grp in contexts:
            item = {"fs_name": fsn, "subvol_name": svn}
            if grp:
                item["group_name"] = grp
            renders.append(item)

        return self.apply_user_templated(
            cluster,
            user_entity=user_entity,
            template_capabilities=template_capabilities,
            renders=renders,
            extra_subs=extra_subs,
            merge_strategy=merge_strategy,
            dry_run=dry_run,
            sync_across_clusters=sync_across_clusters,
            preferred_source=preferred_source,
        )

    @staticmethod
    def _dedup_contexts(contexts: List[Tuple[str, str, Optional[str]]]) -> List[Tuple[str, str, Optional[str]]]:
        seen, out = set(), []
        for fsn, svn, grp in contexts:
            key = (fsn, svn, grp or None)
            if key not in seen:
                seen.add(key);
                out.append((fsn, svn, grp))
        return out

    def apply_user_multi(
            self,
            cluster: str,
            *,
            user_entity: str,
            template_capabilities: List[Dict[str, str]],
            contexts: List[Tuple[str, str, Optional[str]]],
            dry_run: bool = False,
            preferred_source: Optional[str] = None,
            sync_across_clusters: bool = True,
            extra_subs: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        contexts = self._dedup_contexts(contexts)
        return self.apply_user_for_multiple_subvols(
            cluster=cluster,
            user_entity=user_entity,
            template_capabilities=self._normalize_caps(template_capabilities),
            contexts=contexts,
            merge_strategy="multi",
            dry_run=dry_run,
            preferred_source=preferred_source,
            sync_across_clusters=sync_across_clusters,
            extra_subs=extra_subs,
        )

    def list_users(self, cluster: str) -> Dict[str, Any]:
        """GET /cluster/user?cluster=<name>"""
        return self._request("GET", "/cluster/user", params=self._params_with_cluster(cluster))

    def delete_user(self, cluster: str, entity: str) -> Dict[str, Any]:
        """DELETE /cluster/user/{entity}?cluster=<name>"""
        return self._request("DELETE", f"/cluster/user/{entity}", params=self._params_with_cluster(cluster))

    def export_users(self, cluster: str, entities: List[str]) -> Dict[str, Any]:
        """
        POST /cluster/user/export?cluster=<name>
        Returns ExportUsersResponse (e.g., {"clusters": {...}, "type": "keyrings", ...}).
        """
        if not entities:
            raise ValueError("entities must be a non-empty list")
        return self._request(
            "POST",
            "/cluster/user/export",
            params=self._params_with_cluster(cluster),
            json={"entities": entities},
        )

    # --------------------- CephFS (per cluster via query param) ---------------------

    def create_or_resize_subvolume(
        self,
        cluster: str,
        vol_name: str,
        subvol_name: str,
        *,
        group_name: Optional[str] = None,
        size: Optional[int] = None,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """PUT /cephfs/subvolume/{vol_name}?cluster=<name>"""
        payload: Dict[str, Any] = {"subvol_name": subvol_name}
        if group_name:
            payload["group_name"] = group_name
        if size is not None:
            payload["size"] = int(size)
        if mode:
            payload["mode"] = str(mode)
        return self._request(
            "PUT",
            f"/cephfs/subvolume/{vol_name}",
            params=self._params_with_cluster(cluster),
            json=payload,
        )

    def get_subvolume_info(
        self,
        cluster: str,
        vol_name: str,
        subvol_name: str,
        *,
        group_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /cephfs/subvolume/{vol_name}/info?cluster=<name>&subvol_name=..."""
        q = {"subvol_name": subvol_name}
        if group_name:
            q["group_name"] = group_name
        return self._request(
            "GET",
            f"/cephfs/subvolume/{vol_name}/info",
            params=self._params_with_cluster(cluster, q),
        )

    def subvolume_exists(
        self,
        cluster: str,
        vol_name: str,
        subvol_name: str,
        *,
        group_name: Optional[str] = None,
    ) -> bool:
        """GET /cephfs/subvolume/{vol_name}/exists?cluster=<name>&subvol_name=... -> {'exists': bool}"""
        q = {"subvol_name": subvol_name}
        if group_name:
            q["group_name"] = group_name
        res = self._request(
            "GET",
            f"/cephfs/subvolume/{vol_name}/exists",
            params=self._params_with_cluster(cluster, q),
        )
        return bool(res.get("exists")) if isinstance(res, dict) else bool(res)

    def delete_subvolume(
        self,
        cluster: str,
        vol_name: str,
        subvol_name: str,
        *,
        group_name: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """DELETE /cephfs/subvolume/{vol_name}?cluster=<name>&subvol_name=..."""
        q: Dict[str, Any] = {"subvol_name": subvol_name, "force": str(bool(force)).lower()}
        if group_name:
            q["group_name"] = group_name
        return self._request(
            "DELETE",
            f"/cephfs/subvolume/{vol_name}",
            params=self._params_with_cluster(cluster, q),
        )

    # --------------------- New: /cluster/user (PUT overwrite) ---------------------

    def overwrite_user_caps(
        self,
        cluster: str,
        *,
        user_entity: str,
        capabilities: Union[List[Dict[str, str]], Dict[str, str]],
    ) -> Dict[str, Any]:
        """
        PUT /cluster/user?cluster=<name>
        Overwrite capabilities for an existing CephX user.

        `capabilities` can be:
          - list of {"type": "...", "value": "..."} (preferred), or
          - list of {"entity": "...", "cap": "..."} (accepted), or
          - dict {"mds": "allow ...", "mon": "allow r", ...}
        """
        caps_norm = self._normalize_kv_caps(capabilities)
        payload = {"user_entity": user_entity, "capabilities": caps_norm}
        return self._request(
            "PUT",
            "/cluster/user",
            params=self._params_with_cluster(cluster),
            json=payload,
        )

    # --------------------- New: CephFS listing ---------------------

    def list_subvolumes(
        self,
        cluster: str,
        vol_name: str,
        *,
        group_name: Optional[str] = None,
        info: bool = False,
    ) -> Dict[str, Any]:
        """
        GET /cephfs/subvolume/{vol_name}?cluster=<name>[&group_name=...][&info=true]
        Returns the SubvolumeList envelope (with `data` holding names or objects).
        """
        q: Dict[str, Any] = {}
        if group_name:
            q["group_name"] = group_name
        if info:
            q["info"] = "true"
        return self._request(
            "GET",
            f"/cephfs/subvolume/{vol_name}",
            params=self._params_with_cluster(cluster, q),
        )

    def list_subvolume_groups(
        self,
        cluster: str,
        vol_name: str,
        *,
        info: bool = False,
    ) -> Dict[str, Any]:
        """
        GET /cephfs/subvolume/group/{vol_name}?cluster=<name>[&info=true]
        Returns the SubvolumeGroupList envelope (with `data` holding names or objects).
        """
        q: Dict[str, Any] = {}
        if info:
            q["info"] = "true"
        return self._request(
            "GET",
            f"/cephfs/subvolume/group/{vol_name}",
            params=self._params_with_cluster(cluster, q),
        )


    def delete_subvolume_group(
        self,
        cluster: str,
        vol_name: str,
        group_name: str,
        *,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        DELETE /cephfs/subvolume/{vol_name}/group?cluster=<name>&group_name=...&force=(true|false)

        Removes a subvolume group. Behavior:
        - If force=false, server should require the group to be empty.
        - If force=true, server may delete remaining subvolumes (implementation-specific).
        """
        if not group_name or not group_name.strip():
            raise ValueError("group_name must be a non-empty string")
        q = {"group_name": group_name, "force": str(bool(force)).lower()}
        try:
            return self._request(
                "DELETE",
                f"/cephfs/subvolume/{vol_name}/group",
                params=self._params_with_cluster(cluster, q),
            )
        except ApiError as e:
            # Graceful hint for GUI
            if e.status == 404:
                return {"status": 404, "message": "Group not found", "group_name": group_name}
            raise