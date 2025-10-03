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
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse

import requests

from fabric_ceph.common.config import ClusterEntry

ACCEPT = "application/vnd.ceph.api.v1.0+json"


@dataclass
class DashClient:
    cluster_name: str
    cluster: ClusterEntry
    token: str
    verify_tls: bool

    @classmethod
    def for_cluster(cls, name: str, cluster: ClusterEntry) -> "DashClient":
        # Default verify: True for HTTPS endpoints, False for HTTP endpoints
        parsed = urlparse(cluster.dashboard.primary_endpoint)
        '''
        verify_tls_env = os.getenv(f"{name.upper().replace('-', '_')}_VERIFY_TLS")
        verify_tls_default = (parsed.scheme == "https")
        verify_tls = (
            verify_tls_default
            if verify_tls_env is None
            else verify_tls_env.strip().lower() in {"1", "true", "yes", "on"}
        )
        '''
        verify_tls = False
        token = cluster.dashboard.login_get_jwt(verify_tls=verify_tls)
        return cls(name, cluster, token, verify_tls)

    @property
    def base_api(self) -> str:
        return self.cluster.dashboard.base_api_url

    def _hdrs(self) -> Dict[str, str]:
        return {
            "Accept": ACCEPT,
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def list_users(self) -> List[Dict]:
        r = requests.get(f"{self.base_api}/cluster/user", headers=self._hdrs(), timeout=60, verify=self.verify_tls)
        r.raise_for_status()
        js = r.json()
        if isinstance(js, list):
            return js
        if isinstance(js, dict) and "data" in js and isinstance(js["data"], list):
            return js["data"]
        return []

    def delete_user(self, entity: str) -> Tuple[bool, Optional[str]]:
        r = requests.delete(f"{self.base_api}/cluster/user/{entity}", headers=self._hdrs(), timeout=60,
                            verify=self.verify_tls)
        if r.status_code in (200, 202, 204):
            return True, None
        # common not-found codes
        if r.status_code in (400, 404):
            try:
                return False, r.json().get("detail") or r.text
            except Exception:
                return False, r.text
        # treat others as errors
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RuntimeError(f"delete_user failed {r.status_code}: {detail}")

    def create_user(self, user_entity: str, capabilities: List[Dict[str, str]]) -> int:
        payload = {"user_entity": user_entity, "capabilities": capabilities}
        r = requests.post(f"{self.base_api}/cluster/user", headers=self._hdrs(), json=payload, timeout=60, verify=self.verify_tls)
        if r.status_code not in (200, 201, 202):
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise RuntimeError(f"[{self.cluster_name}] create_user failed: {r.status_code} {detail}")
        return r.status_code

    def export_keyring(self, user_entity: str) -> str:
        payload = {"entities": [user_entity]}
        r = requests.post(f"{self.base_api}/cluster/user/export", headers=self._hdrs(), json=payload, timeout=60, verify=self.verify_tls)
        r.raise_for_status()
        try:
            js = r.json()
            if isinstance(js, dict):
                # common shapes: {"keyring": "..."} or raw string
                return js.get("keyring") or js.get("result") or js.get("output") or r.text
        except Exception:
            pass
        return r.text

    def update_user_caps(self, user_entity: str, capabilities: List[Dict[str, str]]) -> int:
        """
        PUT /cluster/user to overwrite capabilities.
        Returns HTTP status code. 200/201/202 = OK; 400/404 often mean user missing.
        """
        payload = {"user_entity": user_entity, "capabilities": capabilities}
        r = requests.put(f"{self.base_api}/cluster/user", headers=self._hdrs(),
                         json=payload, timeout=60, verify=self.verify_tls)
        return r.status_code

    # --- CephFS helpers ---

    def ensure_subvol_group(self, fs_name: str, group_name: str) -> None:
        """
        POST /cephfs/subvolume/group { vol_name, group_name }
        Safe to call repeatedly.
        """
        url = f"{self.base_api}/cephfs/subvolume/group"
        payload = {"vol_name": fs_name, "group_name": group_name}
        r = requests.post(url, headers=self._hdrs(), json=payload, timeout=60, verify=self.verify_tls)
        # many dashboards return 200/201/202; treat 400 with existing group as OK
        if r.status_code in (200, 201, 202, 204):
            return
        if r.status_code == 400:
            # Best-effort idempotency: try an info call to confirm presence
            try:
                _ = self.get_subvolume_info(fs_name, "__nonexistent__", group_name)  # no-op; will likely 404
            except Exception:
                pass
            # If group truly missing, the next create_or_resize will fail anyway
            return
        r.raise_for_status()

    def create_subvolume(
        self,
        fs_name: str,
        subvol_name: str,
        group_name: str | None = None,
        size_bytes: int | None = None,
        mode: str | None = None,
    ) -> None:
        """
        PUT /cephfs/subvolume/{vol_name}
        - If subvol doesn't exist -> create (mode honored if provided)
        - If exists -> resize if size_bytes provided (mode ignored by server)
        """
        url = f"{self.base_api}/cephfs/subvolume"
        payload = {"vol_name": fs_name, "subvol_name": subvol_name}
        if group_name:
            payload["group_name"] = group_name
        if size_bytes is not None and int(size_bytes) > 0:
            payload["size"] = int(size_bytes)
        if mode:
            payload["mode"] = str(mode)
        r = requests.post(url, headers=self._hdrs(), json=payload, timeout=60, verify=self.verify_tls)
        if r.status_code not in (200, 201, 202):
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise RuntimeError(f"[{self.cluster_name}] subvolume create/resize failed: {r.status_code} {detail}")

    def resize_subvolume(
        self,
        fs_name: str,
        subvol_name: str,
        group_name: str | None = None,
        size_bytes: int | None = None,
        mode: str | None = None,
    ) -> None:
        """
        PUT /cephfs/subvolume/{vol_name}
        - If subvol doesn't exist -> create (mode honored if provided)
        - If exists -> resize if size_bytes provided (mode ignored by server)
        """
        url = f"{self.base_api}/cephfs/subvolume/{fs_name}"
        payload = {"subvol_name": subvol_name}
        if group_name:
            payload["group_name"] = group_name
        if size_bytes is not None and int(size_bytes) > 0:
            payload["size"] = int(size_bytes)
        if mode:
            payload["mode"] = str(mode)
        r = requests.put(url, headers=self._hdrs(), json=payload, timeout=60, verify=self.verify_tls)
        if r.status_code not in (200, 201, 202):
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise RuntimeError(f"[{self.cluster_name}] subvolume create/resize failed: {r.status_code} {detail}")

    def get_subvolume_info(self, fs_name: str, subvol_name: str, group_name: str | None = None) -> dict:
        """
        GET /cephfs/subvolume/{vol_name}/info?subvol_name=&group_name=
        Returns the dashboard's JSON (expects 'path' key to be present).
        """
        url = f"{self.base_api}/cephfs/subvolume/{fs_name}/info"
        params = {"subvol_name": subvol_name}
        if group_name:
            params["group_name"] = group_name
        r = requests.get(url, headers=self._hdrs(), params=params, timeout=60, verify=self.verify_tls)
        r.raise_for_status()
        return r.json()

    def subvolume_exists(self, fs_name: str, subvol_name: str, group_name: str | None = None) -> bool:
        """
        GET /cephfs/subvolume/{vol_name}/exists?subvol_name=&group_name=
        """
        url = f"{self.base_api}/cephfs/subvolume/{fs_name}/exists"
        params = {"subvol_name": subvol_name}
        if group_name:
            params["group_name"] = group_name
        r = requests.get(url, headers=self._hdrs(), params=params, timeout=30, verify=self.verify_tls)
        if r.status_code == 200:
            try:
                js = r.json()
                if isinstance(js, dict) and "exists" in js:
                    return bool(js["exists"])
            except Exception:
                pass
        # Fallback: try info (200 -> exists; 404 -> not)
        try:
            _ = self.get_subvolume_info(fs_name, subvol_name, group_name)
            return True
        except Exception:
            return False

    def delete_subvolume(self, fs_name: str, subvol_name: str, group_name: str | None = None, force: bool = False) -> None:
        """
        DELETE /cephfs/subvolume/{vol_name}?subvol_name=&group_name=&force=
        """
        url = f"{self.base_api}/cephfs/subvolume/{fs_name}"
        params = {"subvol_name": subvol_name, "force": str(bool(force)).lower()}
        if group_name:
            params["group_name"] = group_name
        r = requests.delete(url, headers=self._hdrs(), params=params, timeout=60, verify=self.verify_tls)
        if r.status_code not in (200, 202, 204):
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise RuntimeError(f"[{self.cluster_name}] subvolume delete failed: {r.status_code} {detail}")
