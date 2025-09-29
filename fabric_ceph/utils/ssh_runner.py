import os
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlparse

import paramiko

from fabric_ceph.common.config import ClusterEntry


@dataclass
class SSHCreds:
    host: str
    port: int = 22
    user: str = "root"
    key_path: Optional[str] = None
    password: Optional[str] = None

    @classmethod
    def for_cluster(cls, cluster_name: str, cluster: ClusterEntry) -> "SSHCreds":
        env = cluster_name.upper().replace("-", "_")
        # Derive host from dashboard endpoint if not overridden
        parsed = urlparse(cluster.dashboard.primary_endpoint)
        default_host = parsed.hostname or "localhost"
        default_ssh_user = cluster.dashboard.ssh_user
        default_key = cluster.dashboard.ssh_key
        default_port = cluster.dashboard.ssh_port

        host = os.getenv(f"{env}_SSH_HOST", default_host)
        port = int(os.getenv(f"{env}_SSH_PORT", default_port))
        user = os.getenv(f"{env}_SSH_USER", default_ssh_user)
        key_path = os.getenv(f"{env}_SSH_KEY", os.path.expanduser(default_key))

        return cls(host=host, port=port, user=user, key_path=key_path)


class SSHRunner:
    def __init__(self, creds: SSHCreds, timeout: int = 30):
        self.creds = creds
        self.timeout = timeout
        self.client: Optional[paramiko.SSHClient] = None
        self.sftp = None

    def __enter__(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            if self.creds.password:
                self.client.connect(
                    self.creds.host,
                    port=self.creds.port,
                    username=self.creds.user,
                    password=self.creds.password,
                    timeout=self.timeout,
                    look_for_keys=False,
                )
            else:
                pkey = None
                if self.creds.key_path and os.path.exists(self.creds.key_path):
                    try:
                        # Try both Ed25519 and RSA transparently
                        try:
                            pkey = paramiko.Ed25519Key.from_private_key_file(self.creds.key_path)
                        except Exception:
                            pkey = paramiko.RSAKey.from_private_key_file(self.creds.key_path)
                    except Exception as e:
                        raise RuntimeError(f"Failed to load SSH key {self.creds.key_path}: {e}")
                self.client.connect(
                    self.creds.host,
                    port=self.creds.port,
                    username=self.creds.user,
                    pkey=pkey,
                    timeout=self.timeout,
                )
            self.sftp = self.client.open_sftp()
            return self
        except Exception:
            # Clean up partially opened connections
            self.close()
            raise

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        try:
            if self.sftp:  # type: ignore[truthy-function]
                self.sftp.close()
        except Exception:
            pass
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass

    def run(self, cmd: str, check: bool = True) -> Tuple[int, str, str]:
        assert self.client
        stdin, stdout, stderr = self.client.exec_command(cmd, get_pty=False)
        out = stdout.read().decode("utf-8", "ignore")
        err = stderr.read().decode("utf-8", "ignore")
        code = stdout.channel.recv_exit_status()
        if check and code != 0:
            raise RuntimeError(f"SSH command failed ({code}): {cmd}\nSTDERR: {err}\nSTDOUT: {out}")
        return code, out, err

    def put_bytes(self, data: bytes, remote_path: str) -> None:
        assert self.sftp
        with self.sftp.file(remote_path, "wb") as f:
            f.write(data)
        # Set restrictive perms
        self.run(f"chmod 600 {remote_path}")
