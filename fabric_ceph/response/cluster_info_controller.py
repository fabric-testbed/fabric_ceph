from __future__ import annotations

from typing import Dict, Any, List, Optional

from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models import ClusterInfoList, ClusterInfoItem
from fabric_ceph.utils.dash_client import DashClient
from fabric_ceph.utils.utils import cors_success_response, cors_error_response


def _parse_mon_map(mon_json: Dict[str, Any]) -> List[Dict[str, Optional[str]]]:
    """
    Returns a list of {name, v2, v1} from /api/monitor payload.
    Works for addrvec (msgr2+msgr1) and falls back if only v1 is present.
    """
    mons_out: List[Dict[str, Optional[str]]] = []
    mons = (mon_json or {}).get("monmap", {}).get("mons", [])
    for m in mons:
        name = m.get("name") or m.get("rank")  # best-effort
        v1 = None
        v2 = None
        addrvec = ((m.get("public_addrs") or {}).get("addrvec") or [])
        for a in addrvec:
            t = a.get("type")
            addr = a.get("addr")
            if t == "v2":
                v2 = addr
            elif t == "v1":
                v1 = addr
        if not addrvec:
            # very old shape: "public_addr": "IP:6789/0"
            v1 = m.get("public_addr")
        mons_out.append({"name": name, "v2": v2, "v1": v1})
    return mons_out

def _format_mon_host(mons: List[Dict[str, Optional[str]]]) -> str:
    parts = []
    for m in mons:
        v2 = m.get("v2")
        v1 = m.get("v1")
        if v2 and v1:
            parts.append(f"[v2:{v2},v1:{v1}]")
        elif v2:
            parts.append(f"[v2:{v2}]")
        elif v1:
            parts.append(f"[v1:{v1}]")
    return " ".join(parts)

def list_cluster_info():
    """
    GET /cluster/info
    Returns per-cluster fsid + mon endpoints, plus a minimal ceph.conf snippet.
    """
    g = get_globals()
    log = g.log
    try:
        cfg = g.config
        items: List[Dict[str, Any]] = []

        # Stable order across clusters
        clients: Dict[str, DashClient] = {name: DashClient.for_cluster(name, entry)
                                          for name, entry in cfg.cluster.items()}

        for name, dc in clients.items():
            try:
                fsid = dc.get_cluster_fsid()
                mon_json = dc.get_monitor_map()
                mons = _parse_mon_map(mon_json)
                mon_host = _format_mon_host(mons)
                ceph_conf = (
                    f"[global]\n"
                    f"\tfsid = {fsid}\n"
                    f"\tmon_host = {mon_host}\n"
                )
                items.append({
                    "cluster": name,
                    "fsid": fsid,
                    "mons": mons,
                    "mon_host": mon_host,
                    "ceph_conf_minimal": ceph_conf,
                    "error": None,
                })
            except Exception as e:
                log.exception("Failed to fetch cluster info for %s", name)
                items.append({
                    "cluster": name,
                    "fsid": None,
                    "mons": [],
                    "mon_host": "",
                    "ceph_conf_minimal": "",
                    "error": str(e),
                })

        response = ClusterInfoList()
        response.data = []
        response.type = 'clusters'
        for c in items:
            cluster = ClusterInfoItem.from_dict(c)
            response.data.append(cluster)
        response.size = len(response.data)
        response.status = 200

        return cors_success_response(response_body=response)

    except Exception as e:
        g.log.exception(e)
        return cors_error_response(error=e)
