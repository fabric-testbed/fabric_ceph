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
    return rc.delete_subvolume(cluster, vol_name, subvol_name, group_name, force)


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


def list_subvolume_groups(cluster, vol_name, info=None):  # noqa: E501
    """List subvolume groups

    Lists subvolume groups for a filesystem. When &#x60;info&#x3D;true&#x60;, implementations may return detailed objects per group; otherwise names.  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param vol_name: CephFS volume name (filesystem)
    :type vol_name: str
    :param info: When true, return detailed objects per group if supported.
    :type info: bool

    :rtype: Union[SubvolumeGroupList, Tuple[SubvolumeGroupList, int], Tuple[SubvolumeGroupList, int, Dict[str, str]]
    """
    return rc.list_subvolume_groups(cluster, vol_name, info=info)


def list_subvolumes(cluster, vol_name, group_name=None, info=None):  # noqa: E501
    """List subvolumes

    Lists subvolumes for a filesystem. If &#x60;group_name&#x60; is passed, results are filtered to that group. When &#x60;info&#x3D;true&#x60;, implementations may return detailed objects instead of simple names.  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param vol_name: CephFS volume name (filesystem)
    :type vol_name: str
    :param group_name: 
    :type group_name: str
    :param info: When true, return detailed objects per subvolume if supported.
    :type info: bool

    :rtype: Union[SubvolumeList, Tuple[SubvolumeList, int], Tuple[SubvolumeList, int, Dict[str, str]]
    """
    return rc.list_subvolumes(cluster, vol_name, group_name, info=info)


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
