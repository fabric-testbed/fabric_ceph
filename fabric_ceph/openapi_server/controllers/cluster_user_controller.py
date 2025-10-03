from fabric_ceph.response import cluster_user_controller as rc


def apply_user_templated(body, x_cluster=None):  # noqa: E501
    """Upsert a CephX user with cluster-specific capabilities

    Creates or updates a CephX user and synchronizes the SAME SECRET across all clusters. Capability strings may include placeholders &#x60;{fs}&#x60;, &#x60;{path}&#x60;, &#x60;{group}&#x60;, &#x60;{subvol}&#x60; which are rendered per cluster using subvolume info (getpath).  # noqa: E501

    :param create_user_templated_request: 
    :type create_user_templated_request: dict | bytes
    :param x_cluster: Optional cluster try-order/scope hint. Server still syncs to all configured clusters unless restricted by deployment policy. 
    :type x_cluster: str

    :rtype: Union[ApplyUserResponse, Tuple[ApplyUserResponse, int], Tuple[ApplyUserResponse, int, Dict[str, str]]
    """
    return rc.apply_user_templated(body, x_cluster=x_cluster)


def delete_user(entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return rc.delete_user(entity)


def export_users(body):  # noqa: E501
    """Export keyring(s) for one or more CephX users

     # noqa: E501

    :param export_users_request: 
    :type export_users_request: dict | bytes

    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    return rc.export_users(body)


def list_users():  # noqa: E501
    """List all CephX users

     # noqa: E501


    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    return rc.list_users()
