# fabric_ceph/services/rgw_admin.py
import requests
from typing import Optional, List, Dict, Any

from fabric_ceph.common.config import ClusterEntry, Config


class RgwError(Exception):
    pass

class RgwNotFound(RgwError):
    pass

class RgwBadRequest(RgwError):
    def __init__(self, msg: str, details: Optional[str] = None):
        super().__init__(msg)
        self.details = details

class RgwAdmin:
    """
    Tiny RGW Admin Ops client using the Admin Ops API.
    Expects config keys like:
      RGW_CLUSTERS = {
        "europe": {
           "endpoint": "https://rgw.example.org/admin",
           "access_key": "...",
           "secret_key": "...",
           "admin_user": "s3admin",
        }
      }
    """
    def __init__(self, name: str, endpoint: str, access_key: str, secret_key: str, admin_user: Optional[str] = None, verify=True):
        self.name = name
        self.endpoint = endpoint.rstrip("/")
        self.session = requests.Session()
        self.session.verify = verify
        # If your RGW is fronted by NGINX/JWT, adapt auth here (e.g., Bearer)
        self.session.auth = (access_key, secret_key)
        self.admin_user = admin_user

    @classmethod
    def from_config(cls, cluster: str, cfg: Config) -> "RgwAdmin":
        c = cfg.get_cluster(cluster)
        if not c:
            raise RgwBadRequest(f"Unknown cluster '{cluster}'")
        return cls(
            name=cluster,
            endpoint=c.rgw_admin.primary_endpoint,
            access_key=c.rgw_admin.admin_access_key,
            secret_key=c.rgw_admin.admin_secret_key,
            admin_user=c.rgw_admin.ssh_user,
            verify=False
        )

    # ---- helpers ----
    def _url(self, path: str) -> str:
        return f"{self.endpoint}{path}"

    def _raise_for_status(self, r: requests.Response) -> None:
        if r.status_code == 404:
            raise RgwNotFound(r.text)
        if r.status_code in (400, 409):
            raise RgwBadRequest(f"RGW error {r.status_code}", r.text)
        if r.status_code >= 500:
            raise RgwError(f"RGW error {r.status_code}: {r.text}")
        r.raise_for_status()

    # ---- buckets ----
    def list_buckets(self, owner_uid: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Returns list of dicts:
          { name, owner, num_objects, size_kb, placement_rule, zonegroup, zone, versioning }
        """
        params = {}
        if owner_uid:
            params["uid"] = owner_uid
        r = self.session.get(self._url("/bucket"), params=params)
        self._raise_for_status(r)
        raw = r.json() if r.headers.get("content-type","").startswith("application/json") else []
        out = []
        for b in raw:
            out.append({
                "name": b.get("bucket"),
                "owner": b.get("owner"),
                "num_objects": b.get("usage", {}).get("rgw.main", {}).get("num_objects") or b.get("num_objects"),
                "size_kb": b.get("usage", {}).get("rgw.main", {}).get("size_kb") or b.get("size_kb"),
                "placement_rule": b.get("placement_rule"),
                "zonegroup": b.get("zonegroup"),
                "zone": b.get("zone"),
                "versioning": b.get("versioned") and "Enabled" or "Disabled",
            })
        return out

    def get_bucket(self, bucket: str) -> Dict[str, Any]:
        r = self.session.get(self._url(f"/bucket") , params={"bucket": bucket, "stats": "true"})
        self._raise_for_status(r)
        b = r.json()
        return {
            "name": b.get("bucket"),
            "owner": b.get("owner"),
            "num_objects": b.get("usage", {}).get("rgw.main", {}).get("num_objects") or b.get("num_objects"),
            "size_kb": b.get("usage", {}).get("rgw.main", {}).get("size_kb") or b.get("size_kb"),
            "placement_rule": b.get("placement_rule"),
            "zonegroup": b.get("zonegroup"),
            "zone": b.get("zone"),
            "versioning": b.get("versioned") and "Enabled" or "Disabled",
        }

    def create_bucket(self, bucket: str, owner_uid: str, placement_rule: Optional[str], versioning: str) -> Dict[str, Any]:
        payload = {"bucket": bucket, "uid": owner_uid}
        if placement_rule:
            payload["placement_rule"] = placement_rule
        r = self.session.put(self._url("/bucket"), json=payload)
        self._raise_for_status(r)

        # Set versioning if requested
        if versioning in ("Enabled", "Suspended", "Disabled"):
            self.set_bucket_versioning(bucket, status=versioning)

        return self.get_bucket(bucket)

    def delete_bucket(self, bucket: str, purge_objects: bool = False) -> None:
        params = {"bucket": bucket}
        if purge_objects:
            params["purge-objects"] = "true"
        r = self.session.delete(self._url("/bucket"), params=params)
        self._raise_for_status(r)

    def set_bucket_versioning(self, bucket: str, status: str) -> None:
        # Many RGW builds expose an S3-compatible toggle via Admin Ops like:
        #   POST /admin/bucket?bucket=<>&versioning=enabled|suspended
        val = status.lower()
        if val not in ("enabled", "suspended", "disabled"):
            raise RgwBadRequest("Invalid versioning status", f"Got {status}")
        # For Disabled → Suspended; RGW treats "disabled" similar to "suspended" in many versions.
        if val == "disabled":
            val = "suspended"
        r = self.session.post(self._url("/bucket"), params={"bucket": bucket, "versioning": val})
        self._raise_for_status(r)

    def set_bucket_quota(self, bucket: str, enabled: bool, max_size_kb: Optional[int], max_objects: Optional[int]) -> None:
        payload = {
            "bucket": bucket,
            "enabled": bool(enabled),
        }
        if max_size_kb is not None:
            payload["max_size_kb"] = int(max_size_kb)
        if max_objects is not None:
            payload["max_objects"] = int(max_objects)
        r = self.session.put(self._url("/bucket/quota"), json=payload)
        self._raise_for_status(r)
