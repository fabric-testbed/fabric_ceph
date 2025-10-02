import fabric_ceph.response.ceph_fs_controller as rc


def create_or_resize_subvolume(vol_name, body):  # noqa: E501
    """Create or resize a subvolume

    Creates a new subvolume (if it does not exist) or resizes an existing one. Omit &#x60;size&#x60; to create without a quota (unlimited). Send &#x60;size&#x60; (bytes) to set/resize the quota.  # noqa: E501

    :param vol_name: CephFS volume name (filesystem), e.g. &#x60;CEPH-FS-01&#x60;
    :type vol_name: str
    :param subvolume_create_or_resize_request:
    :type subvolume_create_or_resize_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.create_or_resize_subvolume(vol_name, body=body)

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
    return rc.delete_subvolume(vol_name, subvol_name, group_name, force)


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
    return rc.get_subvolume_info(vol_name, subvol_name, group_name)


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
    return rc.subvolume_exists(vol_name, subvol_name, group_name)
