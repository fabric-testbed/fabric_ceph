from http.client import NOT_FOUND

import connexion

from fabric_ceph.common.config import Config
from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models import (
    SubvolumeExists,
    Status200OkNoContentData,
    Status200OkNoContent,
)
from fabric_ceph.openapi_server.models.subvolume_create_or_resize_request import (
    SubvolumeCreateOrResizeRequest,  # noqa: E501
)
from fabric_ceph.response.ceph_exception import CephException
from fabric_ceph.response.cors_response import cors_401, cors_400, cors_500
from fabric_ceph.utils.utils import cors_success_response, cors_error_response, authorize
from fabric_ceph.utils.ceph_fs_helper import (
    ensure_subvolume_on_cluster,
    delete_subvolume_on_cluster,
)
from fabric_ceph.utils.dash_client import DashClient


def _require_known_cluster(cfg: Config, cluster: str):
    """Small helper to validate cluster name and return a DashClient."""
    if not cluster or cluster not in cfg.cluster:
        raise CephException(
            f"Unknown cluster '{cluster}'",
            http_error_code=400,
        )
    return DashClient.for_cluster(cluster, cfg.cluster[cluster])


def create_or_resize_subvolume(cluster, vol_name, body):  # noqa: E501
    """Create or resize a subvolume

    Creates a new subvolume (if it does not exist) or resizes an existing one. Omit &#x60;size&#x60; to create without a quota (unlimited). Send &#x60;size&#x60; (bytes) to set/resize the quota.  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param vol_name: CephFS volume name (filesystem), e.g. &#x60;CEPH-FS-01&#x60;
    :type vol_name: str
    :param subvolume_create_or_resize_request:
    :type subvolume_create_or_resize_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephFs create/resize request")

    try:
        fabric_token, is_operator, _ = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        # Parse body
        subvolume_create_or_resize_request = body
        if connexion.request.is_json:
            subvolume_create_or_resize_request = SubvolumeCreateOrResizeRequest.from_dict(
                connexion.request.get_json()
            )  # noqa: E501

        cfg: Config = globals.config

        # Validate the cluster up front
        try:
            _require_known_cluster(cfg, cluster)
        except CephException as ce:
            return cors_400(details=str(ce))

        # Single-cluster ensure
        result = ensure_subvolume_on_cluster(
            cfg=cfg,
            cluster=cluster,
            fs_name=vol_name,
            subvol_name=subvolume_create_or_resize_request.subvol_name,
            group_name=subvolume_create_or_resize_request.group_name,
            size_bytes=subvolume_create_or_resize_request.size,  # 10 GiB; or None/0 for unlimited
            mode=subvolume_create_or_resize_request.mode,        # used only on create, safe to pass always
        )

        errors: dict = (result.get("errors") or {})
        any_error = bool(errors)
        if any_error:
            details = " ".join(f"{k}:{v}" for k, v in errors.items())
            return cors_500(details=details)

        vol_info = Status200OkNoContentData()
        vol_info.message = f"Subvolume {vol_name} created/resized."
        vol_info.details = result
        response = Status200OkNoContent()
        response.data = [vol_info]
        response.size = len(response.data)
        response.status = 200
        response.type = "no_content"
        return cors_success_response(response_body=response)

    except Exception as e:
        log.exception(f"Failed processing CephFs create/resize request: {e}")
        return cors_error_response(error=e)


def delete_subvolume(cluster, vol_name, subvol_name, group_name=None, force=None):  # noqa: E501
    """Delete a subvolume

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param vol_name: CephFS volume name (filesystem)
    :type vol_name: str
    :param subvol_name:
    :type subvol_name: str
    :param group_name:
    :type group_name: str
    :param force: Force delete even if snapshots exist (behavior depends on cluster policy)
    :type force: bool

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephFs delete request")

    try:
        fabric_token, is_operator, _ = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg: Config = globals.config

        try:
            _require_known_cluster(cfg, cluster)
        except CephException as ce:
            return cors_400(details=str(ce))

        # Single-cluster delete
        result = delete_subvolume_on_cluster(
            cfg=cfg,
            cluster=cluster,
            fs_name=vol_name,
            subvol_name=subvol_name,
            group_name=group_name,
            force=bool(force),
            revoke_caps=True
        )

        print("Would revoke:", result.get("caps_revoked_for"))

        errors: dict = (result.get("errors") or {})
        any_error = bool(errors)
        if any_error:
            details = " ".join(f"{k}:{v}" for k, v in errors.items())
            return cors_500(details=details)

        vol_info = Status200OkNoContentData()
        vol_info.message = f"Subvolume {vol_name} deleted."
        vol_info.details = result
        response = Status200OkNoContent()
        response.data = [vol_info]
        response.size = len(response.data)
        response.status = 200
        response.type = "no_content"
        return cors_success_response(response_body=response)

    except Exception as e:
        log.exception(f"Failed processing CephFs delete request: {e}")
        return cors_error_response(error=e)


def get_subvolume_info(cluster, vol_name, subvol_name, group_name=None):  # noqa: E501
    """Get subvolume info (path)

    Returns subvolume details; use the `path` field as the mount path (equivalent to `getpath`). # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param vol_name: CephFS volume name (filesystem)
    :type vol_name: str
    :param subvol_name:
    :type subvol_name: str
    :param group_name:
    :type group_name: str

    :rtype: Union[SubvolumeInfo, Tuple[SubvolumeInfo, int], Tuple[SubvolumeInfo, int, Dict[str, str]]
    """
    g = get_globals()
    log = g.log
    try:
        fabric_token, is_operator, bastion_login = authorize()
        # Allow operator or the subvol owner (assuming subvol_name == bastion_login for user-scoped mounts)
        if not is_operator and subvol_name.lower() != bastion_login.lower():
            log.error(f"{fabric_token.uuid}/{fabric_token.email} is not authorized to access {vol_name}!")
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized to access {vol_name}!")

        cfg: Config = g.config

        # Validate cluster and get client
        try:
            dc = _require_known_cluster(cfg, cluster)
        except CephException as ce:
            return cors_400(details=str(ce))

        try:
            js = dc.get_subvolume_info(vol_name, subvol_name, group_name)
        except Exception as e:
            log.exception("get_subvolume_info failed on %s: %s", cluster, e)
            # Treat as not found for this cluster
            raise CephException("Subvolume not found or info unavailable", http_error_code=NOT_FOUND)

        vol_info = Status200OkNoContentData()
        vol_info.message = f"Subvolume {vol_name} information retrieved."
        vol_info.details = {cluster: js}
        response = Status200OkNoContent()
        response.data = [vol_info]
        response.size = len(response.data)
        response.status = 200
        response.type = "no_content"
        return cors_success_response(response_body=response)

    except CephException as ce:
        if getattr(ce, "http_error_code", None) == NOT_FOUND:
            # Return 404 payload via generic error helper
            g.log.exception(ce)
        return cors_error_response(error=ce)
    except Exception as e:
        g.log.exception(e)
        return cors_error_response(error=e)


def subvolume_exists(cluster, vol_name, subvol_name, group_name=None):  # noqa: E501
    """Check whether a subvolume exists

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param vol_name:
    :type vol_name: str
    :param subvol_name:
    :type subvol_name: str
    :param group_name:
    :type group_name: str

    :rtype: Union[SubvolumeExists, Tuple[SubvolumeExists, int], Tuple[SubvolumeExists, int, Dict[str, str]]
    """
    g = get_globals()
    log = g.log
    try:
        fabric_token, is_operator, bastion_login = authorize()
        # Keep your existing check (as-is), though many deployments compare subvol_name instead.
        if vol_name.lower() != bastion_login.lower() and not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized to access {vol_name}!")

        cfg: Config = g.config

        # Validate cluster and get client
        try:
            dc = _require_known_cluster(cfg, cluster)
        except CephException as ce:
            return cors_400(details=str(ce))

        try:
            exists = dc.subvolume_exists(vol_name, subvol_name, group_name)
        except Exception as e:
            log.exception("subvolume_exists check failed on %s: %s", cluster, e)
            # On error, report exists=false to avoid leaking details
            return cors_success_response(response_body=SubvolumeExists(exists=False))

        resp = SubvolumeExists(exists=bool(exists))
        return cors_success_response(response_body=resp)

    except Exception as e:
        g.log.exception(e)
        return cors_error_response(error=e)
