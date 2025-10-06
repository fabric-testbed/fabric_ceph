import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from fabric_ceph.openapi_server.models.apply_user_response import ApplyUserResponse  # noqa: E501
from fabric_ceph.openapi_server.models.create_user_templated_request import CreateUserTemplatedRequest  # noqa: E501
from fabric_ceph.openapi_server.models.export_users_request import ExportUsersRequest  # noqa: E501
from fabric_ceph.openapi_server.models.export_users_response import ExportUsersResponse  # noqa: E501
from fabric_ceph.openapi_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_ceph.openapi_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_ceph.openapi_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_ceph.openapi_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_ceph.openapi_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_ceph.openapi_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_ceph.openapi_server.models.users import Users  # noqa: E501
from fabric_ceph.openapi_server import util
from fabric_ceph.response import cluster_user_controller as rc

def apply_user_templated(cluster, body):  # noqa: E501
    """Upsert a CephX user with cluster-specific capabilities

    Creates or updates a CephX user and synchronizes the SAME SECRET across all clusters. Capability strings may include placeholders &#x60;{fs}&#x60;, &#x60;{path}&#x60;, &#x60;{group}&#x60;, &#x60;{subvol}&#x60; which are rendered per cluster using subvolume info (getpath).  # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param create_user_templated_request: 
    :type create_user_templated_request: dict | bytes

    :rtype: Union[ApplyUserResponse, Tuple[ApplyUserResponse, int], Tuple[ApplyUserResponse, int, Dict[str, str]]
    """
    create_user_templated_request = body
    if connexion.request.is_json:
        create_user_templated_request = CreateUserTemplatedRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def delete_user(cluster, entity):  # noqa: E501
    """Delete a CephX user

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param entity: CephX entity, e.g., &#x60;client.demo&#x60;
    :type entity: str

    :rtype: Union[Status200OkNoContent, Tuple[Status200OkNoContent, int], Tuple[Status200OkNoContent, int, Dict[str, str]]
    """
    return 'do some magic!'


def export_users(cluster, body):  # noqa: E501
    """Export keyring(s) for one or more CephX users

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str
    :param export_users_request: 
    :type export_users_request: dict | bytes

    :rtype: Union[ExportUsersResponse, Tuple[ExportUsersResponse, int], Tuple[ExportUsersResponse, int, Dict[str, str]]
    """
    export_users_request = body
    if connexion.request.is_json:
        export_users_request = ExportUsersRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def list_users(cluster):  # noqa: E501
    """List all CephX users

     # noqa: E501

    :param cluster: Target cluster/region identifier as defined by the service config.
    :type cluster: str

    :rtype: Union[Users, Tuple[Users, int], Tuple[Users, int, Dict[str, str]]
    """
    return 'do some magic!'
