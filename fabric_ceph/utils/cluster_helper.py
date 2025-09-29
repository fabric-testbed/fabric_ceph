from dataclasses import dataclass
from typing import List, Dict, Optional

from fabric_ceph.common.config import Config, ClusterEntry
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.ssh_runner import SSHCreds, SSHRunner


@dataclass
class SyncResult:
    source_cluster: str
    existed_on_source: bool
    created_on_source: bool
    imported_to: List[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "source_cluster": self.source_cluster,
            "existed_on_source": self.existed_on_source,
            "created_on_source": self.created_on_source,
            "imported_to": self.imported_to,
        }


def ensure_user_across_clusters(
    cfg: Config,
    user_entity: str,
    capabilities: List[Dict[str, str]],
    preferred_source: Optional[str] = None,
) -> Dict[str, object]:
    """
    Ensure `user_entity` exists with the SAME secret on all clusters.

    - If it exists on any cluster, pick that as the source.
    - Else create it on `preferred_source` (or the first cluster).
    - Export keyring from source.
    - Import keyring via SSH on all other clusters.

    Returns a dict summary (source, existed, created, imported_to).
    """
    # Build Dashboard clients for all clusters
    clients: Dict[str, DashClient] = {}
    for name, entry in cfg.cluster.items():
        clients[name] = DashClient.for_cluster(name, entry)

    # 1) Find a source cluster where the user already exists
    source_name: Optional[str] = None
    for name, dc in clients.items():
        try:
            users = dc.list_users()
            if any((u.get("user_entity") or u.get("entity") or u.get("id")) == user_entity for u in users):
                source_name = name
                break
        except Exception as e:
            # Non-fatal: keep looking (log if you have a logger)
            pass

    existed = source_name is not None
    created = False

    # 2) If none found, pick a source and create
    if not source_name:
        if preferred_source and preferred_source in clients:
            source_name = preferred_source
        else:
            # choose the first cluster deterministically
            source_name = next(iter(clients.keys()))
        # Create on source
        clients[source_name].create_user(user_entity, capabilities)
        created = True

    # 3) Export keyring from source
    keyring = clients[source_name].export_keyring(user_entity)
    keyring_bytes = keyring.encode("utf-8")

    # 4) Import everywhere else over SSH using `ceph auth import`
    imported_to: List[str] = []
    for name, dc in clients.items():
        if name == source_name:
            continue
        entry: ClusterEntry = dc.cluster
        ssh = SSHCreds.for_cluster(name, entry)
        remote_tmp = f"/tmp/{user_entity.replace('.', '_')}.keyring.{os.getpid()}"
        with SSHRunner(ssh) as r:
            # upload
            r.put_bytes(keyring_bytes, remote_tmp)
            # import (use ceph_cli from config, default "ceph")
            ceph_cli = entry.ceph_cli or "ceph"
            r.run(f"{ceph_cli} auth import -i {remote_tmp}")
            # cleanup
            r.run(f"rm -f {remote_tmp}", check=False)
        imported_to.append(name)

    return SyncResult(
        source_cluster=source_name,
        existed_on_source=existed,
        created_on_source=created,
        imported_to=imported_to,
    ).to_dict()