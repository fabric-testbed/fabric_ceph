# fabric_ceph_client.py
"""
Ceph Manager Client (Python)
----------------------------

Minimal, friendly wrapper for the FABRIC Ceph Manager service.

New in this version
- Token can be provided via `token` (string) or `token_file` (path to JSON or text file).
- If the file is JSON, the field `id_token` is extracted by default (configurable via `token_key`).
- Helper `refresh_token_from_file()` to re-read the file on demand.
- Also supports environment variables: FABRIC_CEPH_TOKEN, FABRIC_CEPH_TOKEN_FILE.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union
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
    default_x_cluster: Optional[str] = None
    accept: str = "application/json, text/plain"

    # internal
    _session: requests.Session = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        # Normalize base url (no trailing slash)
        self.base_url = self.base_url.rstrip("/")

        # Allow env overrides if args not set
        if not self.token and not self.token_file:
            env_token = os.getenv("FABRIC_CEPH_TOKEN")
            env_token_file = os.getenv("FABRIC_CEPH_TOKEN_FILE")
            if env_token:
                self.token = env_token
            elif env_token_file:
                self.token_file = env_token_file

        # If a token file is provided, load from it now
        if self.token_file and not self.token:
            self.refresh_token_from_file()

        self._session = requests.Session()
        # Robust retries for idempotent verbs
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
        """
        Re-read `token_file` and set `self.token`.
        - If the file is JSON, extract `self.token_key` (default: 'id_token').
        - If not JSON, treat the file as plain-text with the token on a single line.
        """
        if not self.token_file:
            raise ValueError("token_file is not set")

        path = Path(self.token_file)
        if not path.exists():
            raise FileNotFoundError(f"Token file not found: {path}")

        raw = path.read_text(encoding="utf-8").strip()
        try:
            obj = json.loads(raw)
            # Use configured key; fall back to common alternatives if missing
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
            # Plain text file containing the token
            self.token = raw

        if not self.token:
            raise ValueError("Token could not be loaded from token_file")

    # ----- internal request helpers -----

    def _headers(self, extra: Optional[Dict[str, str]] = None, x_cluster: Optional[str] = None) -> Dict[str, str]:
        h = {"Accept": self.accept}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if x_cluster:
            h["X-Cluster"] = x_cluster
        elif self.default_x_cluster:
            h["X-Cluster"] = self.default_x_cluster
        if extra:
            h.update(extra)
        return h

    @staticmethod
    def _is_json(resp: requests.Response) -> bool:
        ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        return ct.endswith("/json") or ct.endswith("+json")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        data: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        x_cluster: Optional[str] = None,
    ) -> Any:
        url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"
        resp = self._session.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json,
            data=data,
            headers=self._headers(headers, x_cluster=x_cluster),
            timeout=self.timeout,
            verify=self.verify,
        )
        if resp.status_code >= 400:
            # Try to parse structured error
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

        
        return resp.json()

    # --------------------- Cluster User ---------------------

    def list_users(self, *, x_cluster: Optional[str] = None) -> Dict[str, Any]:
        """GET /cluster/user"""
        return self._request("GET", "/cluster/user", x_cluster=x_cluster)

    def create_user(
        self, user_entity: str, capabilities: List[Dict[str, str]], *, x_cluster: Optional[str] = None
    ) -> Dict[str, Any]:
        """POST /cluster/user"""
        payload = {"user_entity": user_entity, "capabilities": capabilities}
        return self._request("POST", "/cluster/user", json=payload, x_cluster=x_cluster)

    def update_user(
        self, user_entity: str, capabilities: List[Dict[str, str]], *, x_cluster: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        PUT /cluster/user
        Server-side may perform 'upsert' (create if missing) depending on implementation.
        """
        payload = {"user_entity": user_entity, "capabilities": capabilities}
        return self._request("PUT", "/cluster/user", json=payload, x_cluster=x_cluster)

    def delete_user(self, entity: str, *, x_cluster: Optional[str] = None) -> Dict[str, Any]:
        """DELETE /cluster/user/{entity}"""
        return self._request("DELETE", f"/cluster/user/{entity}", x_cluster=x_cluster)

    def export_users(self, entities: List[str], *, x_cluster: Optional[str] = None) -> str:
        """
        POST /cluster/user/export
        Returns keyring text. Handles both plain-text and JSON-envelope (`{"keyring": "..."}").
        """
        if not entities:
            raise ValueError("entities must be a non-empty list")
        res = self._request("POST", "/cluster/user/export", json={"entities": entities}, x_cluster=x_cluster)
        if isinstance(res, dict) and "keyring" in res:
            return str(res["keyring"])
        if isinstance(res, str):
            return res
        return str(res)

    # --------------------- CephFS (subvolumes) ---------------------

    def create_subvolume_group(self, vol_name: str, group_name: str, *, x_cluster: Optional[str] = None) -> Dict[str, Any]:
        """POST /cephfs/subvolume/group"""
        payload = {"vol_name": vol_name, "group_name": group_name}
        return self._request("POST", "/cephfs/subvolume/group", json=payload, x_cluster=x_cluster)

    def create_or_resize_subvolume(
        self,
        vol_name: str,
        subvol_name: str,
        *,
        group_name: Optional[str] = None,
        size: Optional[int] = None,
        mode: Optional[str] = None,
        x_cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        """PUT /cephfs/subvolume/{vol_name}"""
        payload: Dict[str, Any] = {"subvol_name": subvol_name}
        if group_name:
            payload["group_name"] = group_name
        if size is not None:
            payload["size"] = int(size)
        if mode:
            payload["mode"] = str(mode)
        return self._request("PUT", f"/cephfs/subvolume/{vol_name}", json=payload, x_cluster=x_cluster)

    def get_subvolume_info(
        self, vol_name: str, subvol_name: str, *, group_name: Optional[str] = None, x_cluster: Optional[str] = None
    ) -> Dict[str, Any]:
        """GET /cephfs/subvolume/{vol_name}/info"""
        params = {"subvol_name": subvol_name}
        if group_name:
            params["group_name"] = group_name
        return self._request("GET", f"/cephfs/subvolume/{vol_name}/info", params=params, x_cluster=x_cluster)

    def subvolume_exists(
        self, vol_name: str, subvol_name: str, *, group_name: Optional[str] = None, x_cluster: Optional[str] = None
    ) -> bool:
        """GET /cephfs/subvolume/{vol_name}/exists -> {'exists': bool}"""
        params = {"subvol_name": subvol_name}
        if group_name:
            params["group_name"] = group_name
        res = self._request("GET", f"/cephfs/subvolume/{vol_name}/exists", params=params, x_cluster=x_cluster)
        if isinstance(res, dict) and "exists" in res:
            return bool(res["exists"])
        return bool(res)

    def delete_subvolume(
        self,
        vol_name: str,
        subvol_name: str,
        *,
        group_name: Optional[str] = None,
        force: bool = False,
        x_cluster: Optional[str] = None,
    ) -> Dict[str, Any]:
        """DELETE /cephfs/subvolume/{vol_name}"""
        params: Dict[str, Any] = {"subvol_name": subvol_name, "force": str(bool(force)).lower()}
        if group_name:
            params["group_name"] = group_name
        return self._request("DELETE", f"/cephfs/subvolume/{vol_name}", params=params, x_cluster=x_cluster)
