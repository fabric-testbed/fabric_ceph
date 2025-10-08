from fabric_ceph.response import cluster_user_controller as rc


def apply_user_templated(cluster, body):  # noqa: E501
    """Upsert a CephX user with cluster-specific capabilities (templated)

    Creates or updates a CephX user and synchronizes the SAME SECRET across clusters. Capability strings may include placeholders &#x60;{fs}&#x60;, &#x60;{path}&#x60;, &#x60;{group}&#x60;, &#x60;{subvol}&#x60; which are rendered per cluster using subvolume info (getpath).  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param create_user_templated_request: 
    :type create_user_templated_request: dict | bytes

    :rtype: Union[ApplyUserResponse, Tuple[ApplyUserResponse, int], Tuple[ApplyUserResponse, int, Dict[str, str]]
    """
    return rc.apply_user_templated(cluster, body)


def delete_user(cluster, entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.delete_user(cluster, entity)


def export_users(cluster, body):  # noqa: E501
    """Export keyring(s) for one or more CephX users

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param export_users_request: 
    :type export_users_request: dict | bytes

    :rtype: Union[ExportUsersResponse, Tuple[ExportUsersResponse, int], Tuple[ExportUsersResponse, int, Dict[str, str]]
    """
    return rc.export_users(cluster, body)


def list_users(cluster):  # noqa: E501
    """List all CephX users

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str

    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    return rc.list_users(cluster)


def overwrite_user_caps(cluster, body):  # noqa: E501
    """Overwrite capabilities for an existing CephX user (non-templated)

    Overwrites a user&#39;s capabilities with the provided list. Commonly used to adjust a single component (e.g., &#x60;mds&#x60;) while preserving others (&#x60;mon&#x60;, &#x60;osd&#x60;, &#x60;mgr&#x60;).  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param update_user_caps_request: 
    :type update_user_caps_request: dict | bytes

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.overwrite_user_caps(cluster, body)