import os
import json
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import paramiko
import connexion

ACCEPT = "application/vnd.ceph.api.v1.0+json"

def _parse_entity_from_keyring(keyring_text: str) -> Optional[str]:
    # Looks for a line like: [client.app]
    for line in keyring_text.splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            return line.strip("[]").strip()
    return None

def _dash_login_and_base_api(cluster_entry) -> Tuple[str, str, bool]:
    """
    Returns (token, base_api, verify_tls) for the given cluster entry.
    We do a fresh login per-cluster using dashboard creds from config.
    """
    base = cluster_entry.dashboard.primary_endpoint.rstrip("/") + "/api"
    verify_tls = base.startswith("https://")
    r = requests.post(
        f"{base}/auth",
        headers={"Accept": ACCEPT, "Content-Type": "application/json"},
        json={"username": cluster_entry.dashboard.user, "password": cluster_entry.dashboard.password},
        timeout=60,
        verify=verify_tls,
    )
    r.raise_for_status()
    token = r.json().get("token")
    if not token:
        raise RuntimeError("Dashboard login succeeded but no token returned")
    return token, base, verify_tls

def _list_users(base_api: str, token: str, verify_tls: bool) -> List[Dict[str, Any]]:
    r = requests.get(f"{base_api}/cluster/user",
                     headers={"Accept": ACCEPT, "Authorization": f"Bearer {token}"},
                     timeout=60, verify=verify_tls)
    r.raise_for_status()
    js = r.json()
    if isinstance(js, list):
        return js
    if isinstance(js, dict) and "data" in js and isinstance(js["data"], list):
        return js["data"]
    return []

def _create_user(base_api: str, token: str, verify_tls: bool,
                 user_entity: str, capabilities: List[Dict[str, str]]) -> None:
    payload = {"user_entity": user_entity, "capabilities": capabilities}
    r = requests.post(f"{base_api}/cluster/user",
                      headers={"Accept": ACCEPT, "Authorization": f"Bearer {token}",
                               "Content-Type": "application/json"},
                      json=payload, timeout=60, verify=verify_tls)
    if r.status_code not in (200, 201, 202):
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RuntimeError(f"create_user failed: {r.status_code} {detail}")

def _export_keyring(base_api: str, token: str, verify_tls: bool, user_entity: str) -> str:
    payload = {"entities": [user_entity]}
    r = requests.post(f"{base_api}/cluster/user/export",
                      headers={"Accept": ACCEPT, "Authorization": f"Bearer {token}",
                               "Content-Type": "application/json"},
                      json=payload, timeout=60, verify=verify_tls)
    r.raise_for_status()
    try:
        js = r.json()
        if isinstance(js, dict):
            return js.get("keyring") or js.get("result") or js.get("output") or r.text
    except Exception:
        pass
    return r.text

def _ssh_params_for_cluster(name: str, entry) -> Tuple[str, int, str, Optional[str], Optional[str]]:
    """
    Resolve SSH params.
    Priority:
      1) Env: <CLUSTER>_SSH_HOST/PORT/USER/KEY/PASSWORD
      2) YAML: dashboard.ssh_user/ssh_key (or rgw_admin.*), port if provided
      3) Defaults: host <- dashboard endpoint host, port 22, user 'root', key ~/.ssh/id_rsa
    """
    env_prefix = name.upper().replace("-", "_")
    dash_host = urlparse(entry.dashboard.primary_endpoint).hostname or "localhost"

    host = os.getenv(f"{env_prefix}_SSH_HOST", dash_host)
    port = int(os.getenv(f"{env_prefix}_SSH_PORT", "22"))

    yaml_user = getattr(entry.dashboard, "ssh_user", None) or getattr(entry.rgw_admin, "ssh_user", None)
    yaml_key  = getattr(entry.dashboard, "ssh_key",  None) or getattr(entry.rgw_admin, "ssh_key",  None)
    yaml_port = getattr(entry.dashboard, "ssh_port", None) or getattr(entry.rgw_admin, "ssh_port", None)
    if yaml_port is not None:
        try:
            port = int(yaml_port)
        except Exception:
            pass

    user = os.getenv(f"{env_prefix}_SSH_USER", yaml_user or "root")
    key_path = os.getenv(f"{env_prefix}_SSH_KEY", yaml_key or os.path.expanduser("~/.ssh/id_rsa"))
    password = os.getenv(f"{env_prefix}_SSH_PASSWORD")  # optional; if set, ignore key

    return host, port, user, (None if password else os.path.expanduser(key_path)), password

def _ssh_import_keyring(host: str, port: int, user: str,
                        key_path: Optional[str], password: Optional[str],
                        ceph_cli: str, keyring_text: str, remote_tmp: str) -> None:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        if password:
            client.connect(hostname=host, port=port, username=user, password=password, timeout=30, look_for_keys=False)
        else:
            pkey = None
            if key_path and os.path.exists(key_path):
                try:
                    try:
                        pkey = paramiko.Ed25519Key.from_private_key_file(key_path)
                    except Exception:
                        pkey = paramiko.RSAKey.from_private_key_file(key_path)
                except Exception as e:
                    raise RuntimeError(f"Failed to load SSH key {key_path}: {e}")
            client.connect(hostname=host, port=port, username=user, pkey=pkey, timeout=30)

        sftp = client.open_sftp()
        with sftp.file(remote_tmp, "wb") as f:
            f.write(keyring_text.encode("utf-8"))
        # Import and cleanup
        client.exec_command(f"chmod 600 {remote_tmp}")
        _, stdout, stderr = client.exec_command(f"{ceph_cli} auth import -i {remote_tmp}")
        _ = stdout.read(); _ = stderr.read()
        client.exec_command(f"rm -f {remote_tmp}")
    finally:
        try:
            client.close()
        except Exception:
            pass
