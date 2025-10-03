from http.client import NOT_FOUND
from typing import Dict

import connexion

from fabric_ceph.common.config import Config
from fabric_ceph.common.globals import get_globals
from fabric_ceph.openapi_server.models import SubvolumeInfo, SubvolumeExists, Status200OkNoContentData, \
    Status200OkNoContent
from fabric_ceph.openapi_server.models.subvolume_create_or_resize_request import SubvolumeCreateOrResizeRequest  # noqa: E501
from fabric_ceph.response.ceph_exception import CephException
from fabric_ceph.response.cors_response import cors_401, cors_500
from fabric_ceph.utils.utils import cors_success_response, cors_error_response, ordered_cluster_names, build_clients, \
    authorize
from fabric_ceph.utils.ceph_fs_helper import ensure_subvolume_across_clusters, delete_subvolume_across_clusters


def create_or_resize_subvolume(vol_name, body):  # noqa: E501
    """Create or resize a subvolume

    Creates a new subvolume (if it does not exist) or resizes an existing one. Omit &#x60;size&#x60; to create without a quota (unlimited). Send &#x60;size&#x60; (bytes) to set/resize the quota.  # noqa: E501

    :param vol_name: CephFS volume name (filesystem), e.g. &#x60;CEPH-FS-01&#x60;
    :type vol_name: str
    :param subvolume_create_or_resize_request:
    :type subvolume_create_or_resize_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    globals = get_globals()
    log = globals.log
    log.debug("Processing CephFs create request")

    try:
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        subvolume_create_or_resize_request = body
        if connexion.request.is_json:
            subvolume_create_or_resize_request = SubvolumeCreateOrResizeRequest.from_dict(
                connexion.request.get_json())  # noqa: E501

        cfg = globals.config
        result = ensure_subvolume_across_clusters(cfg=cfg,
                                                  fs_name=vol_name,
                                                  subvol_name=subvolume_create_or_resize_request.subvol_name,
                                                  group_name=subvolume_create_or_resize_request.group_name,
                                                  size_bytes=subvolume_create_or_resize_request.size,  # 10 GiB; or None/0 for unlimited
                                                  mode=subvolume_create_or_resize_request.mode,  # used only on create, safe to pass always
                                                  #preferred_source="europe",
                                                  # optional
                                                  )
        errors: dict = (result.get("errors") or {})
        any_error = bool(errors)
        if any_error:
            details = " ".join(f"{k}:{v}" for k, v in errors.items())
            return cors_500(details=details)

        vol_info = Status200OkNoContentData()
        vol_info.message = f"Subvolume {vol_name} created."
        vol_info.details = result
        response = Status200OkNoContent()
        response.data = [vol_info]
        response.size = len(response.data)
        response.status = 200
        response.type = 'no_content'
        return cors_success_response(response_body=response)
    except Exception as e:
        log.exception(f"Failed processing CephFs create request: {e}")
        return cors_error_response(error=e)


def delete_subvolume(vol_name, subvol_name, group_name=None, force=None):  # noqa: E501
    """Delete a subvolume

     # noqa: E501

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
        fabric_token, is_operator, bastion_login = authorize()
        if not is_operator:
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized!")

        cfg = globals.config
        result = delete_subvolume_across_clusters(cfg=cfg,
                                                  fs_name=vol_name,
                                                  subvol_name=subvol_name,
                                                  group_name=group_name,
                                                  force=force)

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
        response.type = 'no_content'
        return cors_success_response(response_body=response)
    except Exception as e:
        log.exception(f"Failed processing CephFs delete request: {e}")
        return cors_error_response(error=e)


def get_subvolume_info(vol_name, subvol_name, group_name=None):  # noqa: E501
    """Get subvolume info (path)

    Returns subvolume details; use the &#x60;path&#x60; field as the mount path (equivalent to &#x60;getpath&#x60;). # noqa: E501

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
        if vol_name.lower() != bastion_login.lower():
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized to access {vol_name}!")

        cfg: Config = g.config
        names = ordered_cluster_names(cfg)
        clients = build_clients(cfg, names)

        errors: Dict[str, str] = {}
        for name, dc in clients:
            try:
                js = dc.get_subvolume_info(vol_name, subvol_name, group_name)
                # Convert to generated model (allows extra properties)
                info = SubvolumeInfo().from_dict(js)
                # Optional: include which cluster served the info
                return cors_success_response(response_body=info)
            except Exception as e:
                errors[name] = str(e)
                log.exception("get_subvolume_info failed on %s: %s", name, e)
                continue

        # Nothing succeeded
        raise CephException("Subvolume not found or info unavailable on any cluster", http_error_code=NOT_FOUND)
    except Exception as e:
        g.log.exception(e)
        return cors_error_response(error=e)

def subvolume_exists(vol_name, subvol_name, group_name=None):  # noqa: E501
    """Check whether a subvolume exists

     # noqa: E501

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
        if vol_name.lower() != bastion_login.lower():
            return cors_401(details=f"{fabric_token.uuid}/{fabric_token.email} is not authorized to access {vol_name}!")

        cfg: Config = g.config
        names = ordered_cluster_names(cfg)
        clients = build_clients(cfg, names)

        errors: Dict[str, str] = {}
        for name, dc in clients:
            try:
                exists = dc.subvolume_exists(vol_name, subvol_name, group_name)
                resp = SubvolumeExists(exists=bool(exists))
                # If it exists on this cluster, return immediately with cluster metadata
                if exists:
                    return cors_success_response(response_body=resp)
                # If not on this cluster, keep trying others
            except Exception as e:
                errors[name] = str(e)
                log.exception("subvolume_exists check failed on %s: %s", name, e)
                continue

        # If we reach here, it wasn't found on any cluster (or all checks failed)
        # Return exists=false but include diagnostics if any cluster errored.
        return cors_success_response(response_body=SubvolumeExists(exists=False))

    except Exception as e:
        g.log.exception(e)
        return cors_error_response(error=e)
