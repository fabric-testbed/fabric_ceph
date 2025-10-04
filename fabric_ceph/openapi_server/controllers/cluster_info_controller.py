from fabric_ceph.response import cluster_info_controller as rc


def list_cluster_info():  # noqa: E501
    """Get FSID and monitor endpoints for all clusters

     # noqa: E501


    :rtype: Union[ClusterInfoList, Tuple[ClusterInfoList, int], Tuple[ClusterInfoList, int, Dict[str, str]]
    """
    return rc.list_cluster_info()
