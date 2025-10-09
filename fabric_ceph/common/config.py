#!/usr/bin/env python3
"""
config.py

Typed loader for the Ceph/Webapp configuration YAML.

Updates in this version
-----------------------
- RGW admin `endpoints` can now be a mapping of site-name -> URL (preferred) or
  a simple list of URLs. Internally normalized to an ordered dict so the first
  item remains the "primary".
- Fixed a bug where Dashboard ssh_* fields were incorrectly read from rgw_admin.
- Added helpers on RGWAdminConfig:
  - .primary_endpoint
  - .get_endpoint(name)
  - .endpoints_list
- Kept env-prefix overrides for secrets.

Usage:
    cfg = Config.load_from_file("config.yaml")
    cfg.logging.apply()

    ce = cfg.get_cluster("west")

    # Dashboard
    base_api = ce.dashboard.base_api_url
    token = ce.dashboard.login_get_jwt()

    # RGW admin
    rgw = ce.rgw_admin
    ep_primary = rgw.primary_endpoint
    ep_ucsd = rgw.get_endpoint("UCSD")  # or None if not present
    ak, sk = rgw.admin_access_key, rgw.admin_secret_key
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Any, OrderedDict, Tuple
from collections import OrderedDict as _OrderedDict
import os

import yaml
import requests

from fabric_ceph.utils.log_helper import LogHelper

DEFAULT_CONFIG_PATH = os.getenv("APP_CONFIG_PATH", "config.yml")

# ---------- helpers ----------
def _ensure_nonempty_list(xs: Optional[List[str]], name: str) -> List[str]:
    if not xs or not isinstance(xs, list):
        raise ValueError(f"'{name}' must be a non-empty list")
    xs2 = [s for s in (x.strip() for x in xs) if s]
    if not xs2:
        raise ValueError(f"'{name}' is empty after trimming")
    return xs2


def _normalize_endpoints(obj: Any, name: str) -> "OrderedDict[str, str]":
    """
    Accepts either:
      - dict[str, str]: {SITE: "http://host:port", ...}
      - list[str]: ["http://host1:port", "http://host2:port", ...]
    Returns an OrderedDict preserving original order. Validates non-empty and trims.
    """
    if isinstance(obj, dict):
        items: List[Tuple[str, str]] = []
        for k, v in obj.items():
            k2 = str(k).strip()
            v2 = str(v).strip()
            if not k2:
                raise ValueError(f"Empty key in '{name}'")
            if not v2:
                raise ValueError(f"Empty URL for key '{k}' in '{name}'")
            items.append((k2, v2))
        if not items:
            raise ValueError(f"'{name}' map must not be empty")
        return _OrderedDict(items)

    if isinstance(obj, list):
        vals = _ensure_nonempty_list(obj, name)
        return _OrderedDict((f"ep{i+1}", v) for i, v in enumerate(vals))

    raise ValueError(f"'{name}' must be a mapping (name->url) or list of urls")


def parse_hms_to_datetime(hms: str) -> datetime:
    """
    Parse 'HH:MM:SS' to datetime.
    """
    return datetime.strptime(hms, "%H:%M:%S")


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, str):
        return x.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(x)


# ---------- dataclasses ----------
@dataclass
class DashboardConfig:
    endpoints: List[str]
    user: str
    password: str
    env_prefix: Optional[str] = None
    # optional SSH info (for out-of-band ops)
    ssh_user: Optional[str] = None
    ssh_key: Optional[str] = None
    ssh_port: Optional[int] = None

    def __post_init__(self):
        self.endpoints = _ensure_nonempty_list(self.endpoints, "cluster.<name>.dashboard.endpoints")
        if not self.user:
            raise ValueError("cluster.<name>.dashboard.user is required")
        if self.env_prefix:
            self.password = os.getenv(f"{self.env_prefix}_DASHBOARD_PASSWORD", self.password)

    @property
    def primary_endpoint(self) -> str:
        return self.endpoints[0].rstrip("/")

    @property
    def base_api_url(self) -> str:
        return f"{self.primary_endpoint}/api"

    def login_get_jwt(
        self,
        verify_tls: Optional[bool] = None,
        accept: str = "application/vnd.ceph.api.v1.0+json",
    ) -> str:
        """
        POST /auth to obtain JWT token. Returns token string.
        If verify_tls is None, default to scheme: https=True, http=False.
        """
        if verify_tls is None:
            verify_tls = self.primary_endpoint.startswith("https://")

        url = f"{self.base_api_url}/auth"
        resp = requests.post(
            url,
            headers={"Accept": accept, "Content-Type": "application/json"},
            json={"username": self.user, "password": self.password},
            verify=verify_tls,
            timeout=60,
        )
        resp.raise_for_status()
        js = resp.json()
        token = js.get("token")
        if not token:
            raise RuntimeError(f"Login succeeded but no token in response: {js}")
        return token


@dataclass
class RGWAdminConfig:
    # normalized to an OrderedDict[str, str]
    endpoints_map: "OrderedDict[str, str]"
    admin_access_key: str
    admin_secret_key: str
    env_prefix: Optional[str] = None
    # optional SSH info (for out-of-band ops)
    ssh_user: Optional[str] = None
    ssh_key: Optional[str] = None
    ssh_port: Optional[int] = None

    def __post_init__(self):
        if self.env_prefix:
            self.admin_access_key = os.getenv(f"{self.env_prefix}_RGW_ADMIN_ACCESS_KEY", self.admin_access_key)
            self.admin_secret_key = os.getenv(f"{self.env_prefix}_RGW_ADMIN_SECRET_KEY", self.admin_secret_key)
        if not self.endpoints_map:
            raise ValueError("cluster.<name>.rgw_admin.endpoints must not be empty")

    @property
    def endpoints(self) -> "OrderedDict[str, str]":
        """Back-compat alias."""
        return self.endpoints_map

    @property
    def primary_endpoint(self) -> str:
        """First URL in the ordered map (by YAML order)."""
        # next(iter(dict.values())) raises StopIteration if empty, but we validate non-empty in __post_init__
        return next(iter(self.endpoints_map.values())).rstrip("/")

    @property
    def endpoints_list(self) -> List[str]:
        """List of endpoint URLs in order."""
        return [u.rstrip("/") for u in self.endpoints_map.values()]

    def get_endpoint(self, name: str) -> Optional[str]:
        """Lookup by site/key (e.g., 'UCSD')."""
        v = self.endpoints_map.get(name)
        return v.rstrip("/") if v else None


@dataclass
class ClusterEntry:
    ceph_cli: str
    default_fs: str
    dashboard: DashboardConfig
    rgw_admin: RGWAdminConfig

    def __post_init__(self):
        if not self.ceph_cli:
            self.ceph_cli = "ceph"
        if not self.default_fs:
            raise ValueError("cluster.<name>.default_fs is required")


@dataclass
class LoggingConfig:
    log_directory: Path
    log_file: str
    metrics_log_file: str
    log_level: str = "INFO"
    log_retain: int = 5
    log_size: int = 5_000_000
    logger: str = "app"

    def apply(self) -> None:
        """
        Set up rotating file handlers based on this config.
        """
        return LogHelper.make_logger(
            log_dir=self.log_directory,
            log_file=self.log_file,
            log_level=self.log_level,
            log_retain=self.log_retain,
            log_size=self.log_size,
            logger=self.logger,
        )


@dataclass
class OAuthConfig:
    jwks_url: str
    key_refresh: datetime
    verify_exp: bool = True

    @classmethod
    def from_raw(cls, jwks_url: str, key_refresh: str, verify_exp: Any = True) -> "OAuthConfig":
        return cls(jwks_url=jwks_url, key_refresh=parse_hms_to_datetime(key_refresh), verify_exp=_bool(verify_exp))


@dataclass
class CoreAPIConfig:
    enable: bool = False
    host: Optional[str] = None
    token: Optional[str] = None
    env_prefix: Optional[str] = None

    def __post_init__(self):
        if self.env_prefix:
            self.token = os.getenv(f"{self.env_prefix}_CORE_API_TOKEN", self.token)

    def is_enabled(self) -> bool:
        return self.enable and bool(self.host) and bool(self.token)


@dataclass
class RuntimeConfig:
    service_project: Optional[str] = None
    port: Optional[int] = None


@dataclass
class Config:
    cluster: Dict[str, ClusterEntry]
    runtime: RuntimeConfig
    logging: LoggingConfig
    oauth: OAuthConfig
    core_api: CoreAPIConfig

    # ----------- loader -----------
    @classmethod
    def load_from_file(cls, path: str | Path) -> "Config":
        data = yaml.safe_load(Path(path).read_text())
        if not isinstance(data, dict):
            raise ValueError("Top-level YAML must be a mapping")

        # clusters
        clusters_raw = data.get("cluster") or {}
        if not clusters_raw:
            raise ValueError("'cluster' section is required and cannot be empty")

        clusters: Dict[str, ClusterEntry] = {}
        for name, c in clusters_raw.items():
            if not isinstance(c, dict):
                raise ValueError(f"cluster.{name} must be a mapping")

            # Optional per-cluster env prefix to override secrets easily
            env_prefix = str(name).upper().replace("-", "_")

            dash_raw = c.get("dashboard") or {}
            rgw_raw = c.get("rgw_admin") or {}

            dashboard = DashboardConfig(
                endpoints=dash_raw.get("endpoints") or [],
                user=dash_raw.get("user") or "",
                password=dash_raw.get("password") or "",
                ssh_key=dash_raw.get("ssh_key") or None,
                ssh_user=dash_raw.get("ssh_user") or None,
                ssh_port=dash_raw.get("ssh_port") or None,
                env_prefix=env_prefix,
            )

            rgw_endpoints_map = _normalize_endpoints(
                rgw_raw.get("endpoints") or {},
                f"cluster.{name}.rgw_admin.endpoints",
            )

            rgw = RGWAdminConfig(
                endpoints_map=rgw_endpoints_map,
                admin_access_key=rgw_raw.get("admin_access_key") or "",
                admin_secret_key=rgw_raw.get("admin_secret_key") or "",
                ssh_key=rgw_raw.get("ssh_key") or None,
                ssh_user=rgw_raw.get("ssh_user") or None,
                ssh_port=rgw_raw.get("ssh_port") or None,
                env_prefix=env_prefix,
            )

            entry = ClusterEntry(
                ceph_cli=c.get("ceph_cli") or "ceph",
                default_fs=c.get("default_fs") or "",
                dashboard=dashboard,
                rgw_admin=rgw,
            )
            clusters[name] = entry

        # runtime
        runtime_raw = data.get("runtime") or {}
        runtime = RuntimeConfig(
            service_project=runtime_raw.get("service_project"),
            port=runtime_raw.get("port") or 3500,
        )

        # logging
        log_raw = data.get("logging") or {}
        logging_cfg = LoggingConfig(
            log_directory=Path(log_raw.get("log-directory") or "/var/log/ceph"),
            log_file=log_raw.get("log-file") or "actor.log",
            metrics_log_file=log_raw.get("metrics-log-file") or "metrics.log",
            log_level=log_raw.get("log-level") or "INFO",
            log_retain=int(log_raw.get("log-retain") or 5),
            log_size=int(log_raw.get("log-size") or 5_000_000),
            logger=log_raw.get("logger") or "orchestrator",
        )

        # oauth
        oauth_raw = data.get("oauth") or {}
        oauth = OAuthConfig.from_raw(
            jwks_url=oauth_raw.get("jwks-url") or "",
            key_refresh=oauth_raw.get("key-refresh") or "00:10:00",
            verify_exp=oauth_raw.get("verify-exp", True),
        )
        if not oauth.jwks_url:
            raise ValueError("oauth.jwks-url is required")

        # core_api
        core_raw = data.get("core_api") or {}
        core = CoreAPIConfig(
            enable=_bool(core_raw.get("enable", False)),
            host=core_raw.get("host"),
            token=core_raw.get("token"),
            env_prefix="CORE",  # allows CORE_CORE_API_TOKEN env override if desired
        )

        return cls(cluster=clusters, runtime=runtime, logging=logging_cfg, oauth=oauth, core_api=core)

    # ----------- convenience -----------
    def get_cluster(self, name: str) -> ClusterEntry:
        try:
            return self.cluster[name]
        except KeyError:
            raise KeyError(f"Unknown cluster {name!r}. Available: {', '.join(self.cluster.keys())}")

    def default_cluster(self) -> ClusterEntry:
        key = next(iter(self.cluster.keys()))
        return self.cluster[key]


@lru_cache(maxsize=1)
def get_cfg(path: str | Path = DEFAULT_CONFIG_PATH) -> Config:
    """Load once, reuse everywhere."""
    return Config.load_from_file(path)


def init_cfg(path: str | Path) -> Config:
    """Call this once at startup if you want a non-default path or to reload."""
    get_cfg.cache_clear()
    return get_cfg(path)
