import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_ceph.openapi_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.models.subvolume_create_or_resize_request import SubvolumeCreateOrResizeRequest  # noqa: E501
from fabric_ceph.openapi_server.models.subvolume_exists import SubvolumeExists  # noqa: E501
from fabric_ceph.openapi_server import util
from fabric_ceph.response import ceph_fs_controller as rc

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
    return rc.create_or_resize_subvolume(cluster, vol_name, body=body)


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
    return rc.delete_subvolume(cluster, vol_name, subvol_name, group_name, force=force)


def get_subvolume_info(cluster, vol_name, subvol_name, group_name=None):  # noqa: E501
    """Get subvolume info (path)

    Returns subvolume details; use the &#x60;path&#x60; field as the mount path (equivalent to &#x60;getpath&#x60;). # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param vol_name: CephFS volume name (filesystem)
    :type vol_name: str
    :param subvol_name: 
    :type subvol_name: str
    :param group_name: 
    :type group_name: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.get_subvolume_info(cluster, vol_name, subvol_name, group_name)


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
    return rc.subvolume_exists(cluster, vol_name, subvol_name, group_name)
