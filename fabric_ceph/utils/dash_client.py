import os
from dataclasses import dataclass
from typing import Dict, List
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
        verify_tls_env = os.getenv(f"{name.upper().replace('-', '_')}_VERIFY_TLS")
        verify_tls_default = (parsed.scheme == "https")
        verify_tls = (
            verify_tls_default
            if verify_tls_env is None
            else verify_tls_env.strip().lower() in {"1", "true", "yes", "on"}
        )
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


